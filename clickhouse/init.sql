CREATE DATABASE IF NOT EXISTS infra ON CLUSTER infra_cluster;

CREATE TABLE infra.metrics_raw_local ON CLUSTER infra_cluster
(
    date Date,
    ts DateTime,
    host String,
    vm String,
    metric LowCardinality(String),
    value Float64,
    tags Map(String, String)
)
ENGINE = MergeTree
PARTITION BY date
ORDER BY (metric, host, vm, ts)
TTL ts + INTERVAL 2 DAY;

CREATE TABLE infra.metrics_raw ON CLUSTER infra_cluster
AS infra.metrics_raw_local
ENGINE = Distributed(
    infra_cluster,
    infra,
    metrics_raw_local,
    cityHash64(vm)
);

CREATE TABLE infra.metrics_1m_local ON CLUSTER infra_cluster (
    date Date,
    minute DateTime,
    host String,
    vm String,
    metric LowCardinality(String),

    avg_value AggregateFunction(avg, Float64),
    min_value AggregateFunction(min, Float64),
    max_value AggregateFunction(max, Float64),
    sum_value AggregateFunction(sum, Float64),
    cnt_value AggregateFunction(count)
) ENGINE = AggregatingMergeTree()
PARTITION BY date
ORDER BY (metric, host, vm, minute)
TTL minute + INTERVAL 14 DAY;

CREATE TABLE infra.metrics_1m ON CLUSTER infra_cluster
AS infra.metrics_1m_local
ENGINE = Distributed(
    infra_cluster,
    infra,
    metrics_1m_local,
    cityHash64(vm)
);

CREATE MATERIALIZED VIEW infra.mv_metrics_1m_local
    ON CLUSTER infra_cluster
TO infra.metrics_1m_local
AS
SELECT
    date,
    toStartOfMinute(ts) AS minute,
    host,
    vm,
    metric,

    avgState(value) AS avg_value,
    minState(value) AS min_value,
    maxState(value) AS max_value,
    sumState(value) AS sum_value,
    countState()    AS cnt_value
FROM infra.metrics_raw_local
GROUP BY
    date,
    minute,
    host,
    vm,
    metric;

CREATE TABLE infra.metrics_5m_local ON CLUSTER infra_cluster (
    date Date,
    bucket DateTime,
    host String,
    vm String,
    metric LowCardinality(String),

    avg_value AggregateFunction(avg, Float64),
    min_value AggregateFunction(min, Float64),
    max_value AggregateFunction(max, Float64),
    sum_value AggregateFunction(sum, Float64),
    cnt_value AggregateFunction(count)
)
ENGINE = AggregatingMergeTree
PARTITION BY date
ORDER BY (metric, host, vm, bucket)
TTL bucket + INTERVAL 14 DAY;

CREATE MATERIALIZED VIEW infra.mv_metrics_5m_local
    ON CLUSTER infra_cluster
TO infra.metrics_5m_local
AS
SELECT
    date,
    toStartOfFiveMinute(ts) AS bucket,
    host,
    vm,
    metric,

    avgState(value) AS avg_value,
    minState(value) AS min_value,
    maxState(value) AS max_value,
    sumState(value) AS sum_value,
    countState()    AS cnt_value
FROM infra.metrics_raw_local
GROUP BY
    date,
    bucket,
    host,
    vm,
    metric;

CREATE TABLE infra.metrics_1h_local ON CLUSTER infra_cluster (
    date Date,
    bucket DateTime,
    host String,
    vm String,
    metric LowCardinality(String),

    avg_value AggregateFunction(avg, Float64),
    min_value AggregateFunction(min, Float64),
    max_value AggregateFunction(max, Float64),
    sum_value AggregateFunction(sum, Float64),
    cnt_value AggregateFunction(count)
)
ENGINE = AggregatingMergeTree
PARTITION BY date
ORDER BY (metric, host, vm, bucket)
TTL bucket + INTERVAL 14 DAY;

CREATE MATERIALIZED VIEW infra.mv_metrics_1h_local
    ON CLUSTER infra_cluster
TO infra.metrics_1h_local
AS
SELECT
    date,
    toStartOfHour(ts) AS bucket,
    host,
    vm,
    metric,

    avgState(value) AS avg_value,
    minState(value) AS min_value,
    maxState(value) AS max_value,
    sumState(value) AS sum_value,
    countState()    AS cnt_value
FROM infra.metrics_raw_local
GROUP BY
    date,
    bucket,
    host,
    vm,
    metric;
