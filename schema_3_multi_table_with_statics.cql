drop table if exists monasca.measurements;
drop table if exists monasca.metrics_by_name_dimensions;
drop table if exists monasca.metrics_by_bucket_timestamp;


create table monasca.measurements (
    bucket_start timestamp,
    metric_id blob,
    time_stamp timestamp,
    value float,
    value_meta text,
primary key ((bucket_start, metric_id), time_stamp)
);

create table monasca.metrics_by_name_dimensions (
    tenant_id text,
    region text,
    bucket_start timestamp,
    metric_id blob,
    metric_name text,
    dimension_keys_values text,
primary key ((tenant_id, region, bucket_start), metric_id)
);

CREATE CUSTOM INDEX metrics_dimensions_index ON metrics_by_name_dimensions (dimension_keys_values)
USING 'org.apache.cassandra.index.sasi.SASIIndex'
WITH OPTIONS = {'mode': 'CONTAINS'};

CREATE CUSTOM INDEX metrics_names_index ON metrics_by_name_dimensions (metric_name)
USING 'org.apache.cassandra.index.sasi.SASIIndex'
WITH OPTIONS = {'mode': 'CONTAINS'};

create table monasca.metrics_by_bucket_timestamp (
    tenant_id text,
    region text,
    bucket_start timestamp,
    metric_id blob,
    time_stamp timestamp,
primary key ((tenant_id, region, bucket_start), time_stamp, metric_id)
);