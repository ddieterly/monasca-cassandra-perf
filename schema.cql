// metric_id is sha-1 hash of metric_name and dimensions.
// dimension name and value are concatenated then sorted then concatenated into a single dimensions string.
// metric name is prepended to the dimensions string.

// Assumes that queries across nodes are ok because Monasca will be targeting a 3-node cluster.
// Each index is local to a node.

// Replication will cause extra work on replica nodes.

// Requires client-side join. One query to get the set of metric_id's and
// then another query to get the measurements.


drop table if exists monasca.measurements;
drop table if exists monasca.metric;

drop schema if exists monasca;

// Need to tune strategy and replication_factor

create schema monasca with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

use monasca;

// main measurements table

create table monasca.measurements (
    tenant_id text,
    region text,
    bucket_start timestamp,
    metric_id blob,
    time_stamp timestamp,
    value float,
    value_meta text,
primary key ((tenant_id, region, bucket_start, metric_id), time_stamp)
);