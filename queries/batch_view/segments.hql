use bus_routes;

drop table if exists st_times_int;

create table st_times_int as
select distinct
    st.trip_id,
    t.route_id,
    st.arrival_time,
    CAST(split(st.arrival_time, ':')[0] as INT) as arr_h,
    CAST(split(st.arrival_time, ':')[1] as INT) as arr_m,
    CAST(split(st.arrival_time, ':')[2] as INT) as arr_s,
    (CAST(split(st.arrival_time, ':')[0] as INT) * 3600
         + CAST(split(st.arrival_time, ':')[1] as INT) * 60
         + CAST(split(st.arrival_time, ':')[2] as INT)) as arr_secs,
    st.stop_id,
    CAST(st.stop_sequence as int) as stop_sequence,
    st.shape_dist_traveled
from
    stop_times as st,
    trips as t
where
    t.trip_id = st.trip_id
order by
    trip_id desc,
    stop_sequence asc;

drop table if exists segments_int;

create table segments_int as
select
    concat(r.route_id, '_', r.stop_id, '_', nr.stop_id) as rk,
    r.trip_id,
    r.route_id as route_id,
    r.stop_sequence as start_seq,
    nr.stop_sequence as end_seq,
    r.shape_dist_traveled as start_dist,
    nr.shape_dist_traveled as end_dist,
    (nr.shape_dist_traveled - r.shape_dist_traveled) as dist_trav,
    r.stop_id as start_stop_id,
    nr.stop_id as end_stop_id,
    r.arr_h as seg_hour,
    r.arrival_time as start_arrival,
    nr.arrival_time as end_arrival,
    (nr.arr_secs - r.arr_secs) as secs_trav,
    ((nr.shape_dist_traveled - r.shape_dist_traveled) / 5280) / ((nr.arr_secs - r.arr_secs) / 3600) as speed_mph
from
    st_times_int as r,
    st_times_int as nr
where
    r.trip_id = nr.trip_id
and
    nr.stop_sequence = r.stop_sequence + 1
and
    (nr.arr_secs - r.arr_secs) > 0
order by
    r.trip_id desc,
    r.stop_sequence;

drop table if exists speed_aggs_hb;

create table speed_aggs_hb (
    rk STRING,
    route_id STRING,
    seg_hour STRING,
    avg_speed_mph DOUBLE
)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties (
    'hbase.columns.mapping' = ':key,info:route_id,info:seg_hour,info:avg_speed_mph'
    )
    tblproperties (
        'hbase.table.name' = 'speed_aggs_hb'
        );

insert overwrite table speed_aggs_hb
select
    concat(route_id, '_', seg_hour) as rk,
    route_id,
    seg_hour,
    avg(speed_mph) as avg_speed_mph
from
    segments_int
group by
    route_id,
    seg_hour
order by
    route_id asc,
    seg_hour desc;