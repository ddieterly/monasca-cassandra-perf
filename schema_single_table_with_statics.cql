// metric_id is sha-1 hash of metric_name and dimensions.
// dimension name and value are concatenated then sorted then concatenated into a single dimensions string.
// metric name is prepended to the dimensions string.

// Assumes that queries across nodes are ok because Monasca will be targeting a 3-node cluster.
// Each index is local to a node.

// Replication will cause extra work on replica nodes.

// Requires client-side join. One query to get the set of metric_id's and
// then another query to get the measurements.


drop table if exists monasca.measurements;

drop schema if exists monasca;

// Need to tune strategy and replication_factor

create schema monasca with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

use monasca;

// main measurements table

create table monasca.measurements (
    tenant_id text,
    region text,
    bucket_start timestamp,
    name text,
    dimension_keys_values text,
    created_at timestamp static,
    updated_at timestamp static,
    time_stamp timestamp,
    value float,
    value_meta text,
primary key ((tenant_id, region, bucket_start, name, dimension_keys_values), time_stamp)
);

CREATE CUSTOM INDEX metrics_dimensions_index ON measurements (dimension_keys_values)
USING 'org.apache.cassandra.index.sasi.SASIIndex'
WITH OPTIONS = {'mode': 'CONTAINS'};

CREATE CUSTOM INDEX metrics_names_index ON measurements (name)
USING 'org.apache.cassandra.index.sasi.SASIIndex'
WITH OPTIONS = {'mode': 'CONTAINS'};

CREATE CUSTOM INDEX metrics_created_at_index ON measurements (created_at)
USING 'org.apache.cassandra.index.sasi.SASIIndex'
WITH OPTIONS = {'mode': 'SPARSE'};

CREATE CUSTOM INDEX metrics_updated_at_index ON measurements (updated_at)
USING 'org.apache.cassandra.index.sasi.SASIIndex'
WITH OPTIONS = {'mode': 'SPARSE'};