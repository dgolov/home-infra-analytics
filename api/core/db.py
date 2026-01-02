from aiochclient import ChClient
from datetime import datetime

from src.schemas import MetricBatch


class Base:
    def __init__(self, ch: ChClient):
        self.ch = ch


class MetricsRepository(Base):
    async def add_metric(self, data: MetricBatch):
        now = datetime.utcnow()

        rows = [
            {
                "date": now.date().isoformat(),
                "ts": now.strftime("%Y-%m-%d %H:%M:%S"),
                "host": m.host,
                "vm": m.vm,
                "metric": m.metric,
                "value": m.value,
                "tags": m.tags
            }
            for m in data.root
        ]

        await self.ch.execute(
            "INSERT INTO infra.metrics_raw FORMAT JSONEachRow",
            rows
        )
