SELECT
    minute AS time,
    host,
    avgMerge(avg_value) AS value
FROM infra.metrics_1m
WHERE
    metric = 'cpu_usage'
    AND minute >= toDateTime($__from / 1000)
    AND minute <= toDateTime($__to / 1000)
GROUP BY
    time,
    host
ORDER BY time