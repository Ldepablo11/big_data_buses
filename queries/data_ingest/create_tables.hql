create database if not exists bus_routes;
use bus_routes;

drop table if exists stops;

create external table stops (
    stop_id STRING,
    stop_code STRING,
    stop_name STRING,
    stop_desc STRING,
    stop_lat DOUBLE,
    stop_lon DOUBLE,
    location_type INT,
    parent_station STRING,
    wheelchair_boarding INT
)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES (
       "separatorChar" = ",",
        "quoteChar" = "\""
)
stored as textfile
TBLPROPERTIES ("skip.header.line.count"="1");
load data inpath 'ldepablo1/gtfs_data/feed_11_19_25/stops.txt'
into table stops;

drop table if exists routes;

create external table routes (
       route_id STRING,
       route_short_name STRING,
       route_long_name STRING,
       route_type STRING,
       route_url STRING,
       route_color STRING,
       route_text_color STRING
)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES (
       "separatorChar" = ",",
        "quoteChar" = "\""
)
stored as textfile
TBLPROPERTIES ("skip.header.line.count"="1");
load data inpath 'ldepablo1/gtfs_data/feed_11_19_25/routes.txt'
into table routes;

drop table if exists stop_times;

create external table stop_times (
       trip_id STRING,
       arrival_time STRING,
       departure_time STRING,
       stop_id STRING,
       stop_sequence INT,
       stop_headsign STRING,
       pickup_type INT,
       shape_dist_traveled DOUBLE
)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES (
       "separatorChar" = ",",
        "quoteChar" = "\""
)
stored as textfile
TBLPROPERTIES ("skip.header.line.count"="1");
load data inpath 'ldepablo1/gtfs_data/feed_11_19_25/stop_times.txt'
into table stop_times;

drop table if exists trips;

create external table trips (
       route_id STRING,
       service_id STRING,
       trip_id STRING,
       direction_id STRING,
       block_id STRING,
       shape_id STRING,
       direction STRING,
       wheelchair_accessible INT,
       schd_trip_id STRING
)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES (
       "separatorChar" = ",",
        "quoteChar" = "\""
)
stored as textfile
TBLPROPERTIES ("skip.header.line.count"="1");
load data inpath 'ldepablo1/gtfs_data/feed_11_19_25/trips.txt'
into table trips;