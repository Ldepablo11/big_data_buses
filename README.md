# Bus Routes Project

For my final project, I have built a web app that will provide estimated-times-of-arrival (ETAs) for bus routes in the city of Chicago leveraging real time bus locations, as well as heavily processed public data on bus routes, stop times, trips, and bus stops. This concept is heavily inspired from the Mansueto Institute's Stop Watch project which you can find here: https://github.com/mansueto-institute/cta-stop-watch/tree/main/cta-stop-watch.

## Data Sources
This project has two data sources, one for real time speed views and another for static batch views:
- BusTracker API: provides real time locations for CTA buses, updates every 30 seconds, available at https://www.transitchicago.com/developers/bustracker/.
- General Transit Feed Specification (GTFS): provides historical data on CTA public transit including `routes`, `stop_times`, `strips`, and `stops`, available at: https://www.transitchicago.com/developers/bustracker/.

## Ingesting Data
### Ingesting GTFS data into cluster
The data was ingested by locally downloading the GTFS from https://www.transitchicago.com/downloads/sch_data/, and then putting the `stop_times.txt`, `stops.txt`, `routes.txt`, and `trips.txt` directly into the hadoop cluster. Assuming the data is downloaded into your local directory:

`~/gtfs_data`

I added onto the hadoop the following command (e.g., `stop_times.txt`):

`scp gtfs_data/stop_times.txt hadoop@ec2-54-89-237-222.compute-1.amazonaws.com:~/ldepablo1/gtfs_data/feed_11_19_2`

I added them to the Hadoop Distributed File System (HDFS) as follows (e.g., `stop_times.txt`):

`hdfs dfs -mkdir -p ldepablo1/gtfs_data/feed_11_19_25`
`hdfs dfs -put ~/ldepablo1/gtfs_data/feed_11_19_25/stop_times.txt ldepablo1/gtfs_data/feed_11_19_25/stop_times.txt`

I then removed the files living on the hadoop disk to free storage for others:

`rm ~/ldepablo1/gtfs/feed_11_19/*`

### Creating Hive database
This app includes segment, distance, and time processing which can easily be done in Hive Query Language (HQL.) In order to compute the batch and speed views, the raw data needs to exist in Hive where it can be processed and stored in HBase. The `.hql` files that create these tables are in the `queries/data_ingest/` directory. I added them onto the cluster with this command:

`scp queries/data_ingest/*hql hadoop@ec2-54-89-237-222.compute-1.amazonaws.com:~/ldepablo1/hive_queries/data_ingest/`

#### create_tables.hql
This query creates the `bus_routes` database and reads the GTFS Data stored in `.txt` files from HDFS into Hive. This only needs to be run every time a new batch of GTFS data comes out (approximately once a month.)

For first time usage, I logged into the cluster and ran the commands (in that exact order):

`hive -f ~/ldepablo1/hive_queries/data_ingest/create_tables.hql`

## Lambda Archytecture of Project

**Batch Layer**: Processes GTFS data. Computes `speed_aggs_hb` to determine the average speed on a bus route at different times of service. Computes `route_references` to create a static table joining routes to their stop information (i.e., coordinates. name, id.)

**Speed Layer**: Processes user queries and real time bus positions from Bus Tracker. Finds nearest active vehicle to a stop using given route and stop information. Provides distance between nearest vehicle and stop in miles.

**Serving Layer**: Combines nearest vehicle distance in speed layer and speed data in batch layer to compute ETA.

## Computing Batch Views

The batch views are computed using HQL. The `.hql` files that compute these views are in the `queries/batch_view` direcroty.

I added them into the hadoop cluster with:

`scp queries/batch_view/*hql hadoop@ec2-54-89-237-222.compute-1.amazonaws.com :~/ldepablo1/hive_queries/batch_view/`

And compute the batch views with the commands:

`hive -f ~/ldepablo1/hive_queries/batch_view/route_references.hql`
`hive -f ~/ldepablo1/hive_queries/batch_view/segments.hql`

### route_references.hql
This query creates the **first batch view**: a table that joins `routes` to `stops`. The query creates an HBase table called `route_to_stop_hb`. The table is created by joining tables `routes` and `trips` by `route_id`. Then by joining that table with `stop_times` by `trip_id`. Finally, the table is joined with `stops` by `stop_id`. The end product is a table where the fields are:

`rk`, `route_id`, `stop_id`, `stop_name`, `stop_lat`, `stop_lon`

### segments.hql
This query does the heavy speed computing for route speeds at different hours of the day, and requires two intermediate tables. 

The first query creates the intermediary `st_times_int` table that joins the `stop_times` and `trips` tables by `trip_id`. This table breaks down each the hour, minute, and second at the `arrival_time` for each row in in `stop_times`. Then the total amount of seconds passed at `arrival_time` is computed into the column `arr_secs`. This was done to do clean hour matching, and also because GTFS schedules can sometimes exceed 24hrs, which makes it impossible to use UNIX timestamps. The resulting table `st_times_int` has the columns:

`trip_id`, `route_id`, `arrival_time`, `arr_h`, `arr_m`, `arr_s`, `arr_secs`, `stop_sequence`

The second query creates the `segments_int` table, and computes the segments from stop to stop, for each trip, in each route. The table is joining `stop_times` to itself by matching `trip_id` and `stop_sequence`. For each sequence the distance traveled and number of seconds passed between stops in feet is computed. Able to compute these two values, the query computes the speed in miler per hour for each segment. The resulting `segments_int` table has the columns:

`rk`, `trip_id`, `route_id`, `start_seq`, `stop_seq`, `start_dist`, `end_dist`, `dist_trav`, `start_stop_id`, `end_stop_id`, `seg_hour`, `start_arrival`, `end_arrival`, `secs_trav`, `speed_mph`

Finally the **second batch view**: a table that contains the average speed for each route at each hour of service. The query creates an HBase table called `speed_aggs_hb`. The table is created by averaging the `speed_mph` column in `segments_int` by `route_id` and `seg_hour`. The resulting table `speed_aggs_hb` has the columns:

`rk`, `route_id`, `seg_hour`, `avg_speed_mph`

## Computing the Speed Views

This app's speed views are generated with PySpark and computation within the app. The app relies on the Bus Tracker API's `getvehicles` pull. Given a route, it will provide a table with all of vehicles currently moving on that route with their locations.

### Kafka topics

This app uses two Kafka topics: `ldepablo1_vehicles` & `ldepablo1_vehicle_stops`. 

#### ldepablo1_vehicles

When the user chooses a route and a stop, the Kafka producer inside of `app.js` will send a message into `ldepablo1_vehicles` in JSON format containing the inputted route id and stop coordinates. This is the topic that Spark reads with its own Kafka consumer while streaming in order to compute the speed view.

#### ldepablo1_vehicle_stops

Spark has its own Kafka producer. When Spark has computed the speed view (i.e., the closest vehicle and its distance,) it will send a message to `ldepablo_vehicles_stops` with the nearest vehicle's information stored in a JSON format.

### Spark streaming

The app uses Spark to read user inputs and compute distances between vehicles and bus stops, and the script that manages the Spark session is `spark_job.py`. For each given user input that goes into the `ldepablo1_vehicles` topic it makes an API call to find the vehicles, computes which is the closest vehicle using Haversine distance, and makes into a JSON. Finally, the resulting JSON is pushed to the `ldepablo1_vehicles_stops` topic.

### Pulling route stops
With a route given by the user, the speed layer finds all of the stops for that route with the `get_route_stops` function, leveraging the HBase key design as concatenations of `route_id` and `stop_id`. The function uses the HBase client to directly query the `route_to_stop_hb` table to retrieve all of the stops with their locations.

## The Serving Layer
The objective of the serving layer is to estimate the ETA of a given vehicle to a stop, and to push the results into the interface.

### Querying Distance
With a route, and a recorded time of querying, the web app finds the average speed by quering the `speed_aggs_hb` table in HBase with the function `find_avg_speed`.

### Reading Speed View
After receiving an inputted stop id and route, the app queries the `route_to_stop_hb` HBase table to find the stop coordinates. It then sends a Kafka message to `ldepablo1_vehicles` and then waits for the response.

### Computing ETA
The eta is computed by computing the average speed of a route at a given time. The web app computes the eta with the speed from the batch view, and the distance from the speed view.

### Serving data
The web_app uses two `app.get()` methods to request stop routes, and to perform computations on distances. Each of those has its own `.mustache` file that renders the results of the user queries. The user queries are inputted into the public-facing `index.html` for the web app.

## How to use the app

### Preparation

In order to use the app, you must be in the app directory. If it is not on the cluster already, you need to put the `sark_job.py` file on the cluster with this command:

`scp spark/spark_job.py hadoop@ec2-54-89-237-222.compute-1.amazonaws.com:~/ldepablo1/speed_layer/`

### Execution

It requires two different terminals to actually run the app. First, log into the cluster and begin streaming with this command:

`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 ldepablo1/speed_layer/spark_job.py`

Then, in a separate terminal, launch the app from your local machine with this command:

`node app.js 3019 http://ec2-54-89-237-222.compute-1.amazonaws.com:8070/`

Wait for the streaming to start, then you can finally use the app if you enter this link in your browser:

`http://localhost:3019/`

## Project limitations & areas for improvement

This project has many limitations. Ranked from most limiting to least.

### Integrating HBase with Spark

This project's most important limitation is the inability to use HBase and Spark at the same time.

This is an important bottleneck because the nearest vehicle is computed with a for loop in Python. This would be a lot better if HBase had a self-updating table containing vehicle locations. In this manner, Spark would not have to make API calls, and data ingestion could be made entirely separate from the web app.

I originally created a Python script that went through all of the active routes using the API's `getroutes` pull, pulled all of the actively vehicles for each route, and then wrote the updated locations into an HBase table called `vehicles_hb`. This script would run automatically every 30 seconds. This would allow Spark to directly query this table to compute distances. Although promising, this was not feasible for me due to version mismatches between Spark and HBase.

### Automating Batch Views

This project requires manually updating the batch views (downloading, putting in hadoop, putting hdfs, recomputing whenever the new GTFS report comes out. A great improvement for this app would be to automate that process.

# Sources

#### AI:
Used ChatGPT to debug errors relating to Kafka streaming, the web app, and general technical issues with the cluster.

1. Sharma, H. (2021, October 4). Read a JSON stream and write to Kafka — Spark. Medium.
https://waytohksharma.medium.com/read-a-json-stream-and-write-to-kafka-spark-0b6491fa7cdf.

2. TomTalksPython. (2021, December 9). Integrating PySpark with Apache Kafka. Medium.
https://medium.com/tomtalkspython/integrating-pyspark-with-apache-kafka-6c395432e994.

3. Apache Software Foundation. Structured Streaming + Kafka Integration Guide — Spark Documentation.
https://spark.apache.org/docs/latest/streaming/structured-streaming-kafka-integration.html.

4. Node.js Foundation. JavaScript asynchronous programming and callbacks. Node.js.
https://nodejs.org/en/learn/asynchronous-work/javascript-asynchronous-programming-and-callbacks

5. Mohammad. (2020, May 5). How to implement a callback in Node’s app.get function [Stack Overflow post]. Stack Overflow.
https://stackoverflow.com/questions/64712352/how-to-implement-a-callback-in-nodes-app-get-function.

6. avitex. (2011, February 6). Haversine formula in Python (bearing and distance between two GPS points) [Stack Overflow post]. Stack Overflow.
https://stackoverflow.com/questions/4913349/haversine-formula-in-python-bearing-and-distance-between-two-gps-points.

7. Kafka.js Contributors. Consuming — Kafka.js. https://kafka.js.org/docs/consuming.
