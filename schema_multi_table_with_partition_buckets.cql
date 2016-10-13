// metric_id is sha-1 hash of metric_name and dimensions.
// dimension name and value are concatenated then sorted then concatenated into a single dimensions string.
// metric name is prepended to the dimensions string.

// Assumes that queries across nodes are ok because Monasca will be targeting a 3-node cluster.
// Each index is local to a node.

// Replication will cause extra work on replica nodes.

// Requires client-side join. One query to get the set of metric_id's and
// then another query to get the measurements.


drop table if exists monasca.measurements;
drop table if exists monasca.metrics;

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

// main metrics table

create table monasca.metrics (
    tenant_id text,
    region text,
    bucket int,
    created_at timestamp,
    updated_at timestamp,
    metric_id blob,
    metric_name text,
    dimension_keys_values text,
primary key ((tenant_id, region, bucket), created_at, metric_id)
);

CREATE CUSTOM INDEX metrics_dimensions_index ON metrics (dimension_keys_values)
USING 'org.apache.cassandra.index.sasi.SASIIndex'
WITH OPTIONS = {'mode': 'CONTAINS'};

CREATE CUSTOM INDEX metrics_names_index ON metrics (metric_name)
USING 'org.apache.cassandra.index.sasi.SASIIndex'
WITH OPTIONS = {'mode': 'CONTAINS'};

CREATE CUSTOM INDEX metrics_created_at_index ON metrics (created_at)
USING 'org.apache.cassandra.index.sasi.SASIIndex'
WITH OPTIONS = {'mode': 'SPARSE'};

CREATE CUSTOM INDEX metrics_updated_at_index ON metrics (updated_at)
USING 'org.apache.cassandra.index.sasi.SASIIndex'
WITH OPTIONS = {'mode': 'SPARSE'};
