from clickhouse_driver import Client
from datetime import datetime

from src.schemas import Metric


class MetricsRepository:
    @staticmethod
    def add_metric(client: Client, metric: Metric):
        now = datetime.utcnow()
        client.execute(
            "INSERT INTO metrics_raw VALUES",
            [(
                now.date(),
                now,
                metric.host,
                metric.vm,
                metric.metric,
                metric.value,
                metric.tags
            )]
        )
