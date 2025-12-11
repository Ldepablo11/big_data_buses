import json
import requests
from math import radians, cos, sin, asin, sqrt
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import *

api_key = "bEz4dWVWnCtsTAjiQdgWcKB3q"
bootstrap_server = "boot-public-byg.mpcs53014kafka.2siu49.c2.kafka.us-east-1.amazonaws.com:9196"
k_username = "mpcs53014-2025"
k_password = "A3v4rd4@ujjw"

spark = SparkSession.builder.appName("Find_Nearest_Vehicle").getOrCreate()


def haversine(lat_v, lon_v, lat_s, lon_s):

    lat_v, lon_v, lat_s, lon_s = map(float, [lat_v, lon_v, lat_s, lon_s])
    lat_v, lon_v, lat_s, lon_s = map(radians, [lat_v, lon_v, lat_s, lon_s])
    d_lon = lon_s - lon_v
    d_lat = lat_s - lat_v
    a = sin(d_lat/2)**2 + cos(lat_s) * cos(lat_v) * sin(d_lon/2)**2
    c = 2 * asin(sqrt(a))
    return 3956 * c


def fetch_vehicles(route):

    base = "https://www.ctabustracker.com/bustime/api/v3/getvehicles"
    params = {"key": api_key, "format": "json", "tmres": "s", "rt": route}

    try:
        resp = requests.get(base, params=params).json()
        return resp["bustime-response"]["vehicle"]
    except Exception:
        return []


def find_closest_vehicle(vehicles, stop_lat, stop_lon):

    best_v = None
    best_dist = float("inf")

    for v in vehicles:
        dist = haversine(v["lat"], v["lon"], stop_lat, stop_lon)
        if dist < best_dist:
            best_dist = dist
            best_v = v

    return best_v, best_dist


# Reading the latest Kafka topics from `ldepblo1_vehicles`
kafka_user_inputs = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrap_server)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.scram.ScramLoginModule required '
        f'username="{k_username}" password="{k_password}";'
    )
    .option("subscribe", "ldepablo1_vehicles")
    .load()
)

schema = StructType([
    StructField("route", StringType()),
    StructField("stop_lat", DoubleType()),
    StructField("stop_lon", DoubleType()),
    StructField("ts", LongType())
])

processed_df = (
    kafka_user_inputs
    .selectExpr("CAST(value AS STRING) AS raw")
    .select(from_json("raw", schema).alias("data"))
    .select("data.*")
)

# Function to generate speed view
def process_batch(df, batch_id):
    rows = df.collect()
    out_rows = []

    for row in rows:
        route = row.route
        stop_lat = row.stop_lat
        stop_lon = row.stop_lon

        # Fetch vehicles for this route
        vehicles = fetch_vehicles(route)
        if not vehicles:
            continue

        # Find closest vehicle
        best_v, best_dist = find_closest_vehicle(vehicles, stop_lat, stop_lon)
        if not best_v:
            continue

        # Constructing kafka message
        msg = {
            "vid": best_v["vid"],
            "rt": best_v["rt"],
            "des": best_v["des"],
            "tmstmp": best_v["tmstmp"],
            "distance_miles": best_dist,
            "eta_min": None,
            "avg_speed_mph": None
        }

        out_rows.append((json.dumps(msg),))

    if out_rows:
        out_df = spark.createDataFrame(out_rows, ["value"])
        (
            out_df
            .selectExpr("CAST(value AS BINARY)")
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_server)
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
            .option("kafka.sasl.jaas.config",
                    f'org.apache.kafka.common.security.scram.ScramLoginModule required '
                    f'username="{k_username}" password="{k_password}";')
            .option("topic", "ldepablo1_vehicle_stops")
            .save()
        )

# Executing the stream
query = (
    processed_df
    .writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", "/home/hadoop/ldepablo1/checkpoints/vehicle_stream")
    .start()
)

query.awaitTermination()