use bus_routes;

drop table if exists route_to_stop_hb;

create table route_to_stop_hb (
       rk STRING,
       route_id STRING,
       stop_id STRING,
       stop_name STRING,
       stop_lat DOUBLE,
       stop_lon DOUBLE
)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with SERDEPROPERTIES (
       'hbase.columns.mapping' = ':key,info:route_id,info:stop_id,info:stop_name,info:stop_lat,info:stop_lon'
)
TBLPROPERTIES (
       'hbase.table.name' = 'route_to_stop_hb'
);

insert overwrite table route_to_stop_hb
select distinct
    concat(r.route_id, '_', st.stop_id) as rk,
    r.route_id,
    st.stop_id,
    s.stop_name,
    s.stop_lat,
    s.stop_lon
from
    routes as r,
    stop_times as st,
    stops as s,
    trips as t
where
    r.route_id = t.route_id
and
    t.trip_id = st.trip_id
and
    st.stop_id = s.stop_id
order by
    route_id asc,
    stop_id desc;