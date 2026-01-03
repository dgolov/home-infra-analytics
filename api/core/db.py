from abc import ABC, abstractmethod
from aiochclient import ChClient
from datetime import datetime

from src.schemas import MetricBatch, MetricsQuery


class BaseMetricsRepository:
    def __init__(self, ch: ChClient):
        self.ch = ch


class BaseMetricsWriteRepository(ABC, BaseMetricsRepository):
    @abstractmethod
    async def add_metric(self, data: MetricBatch):
        ...


class BaseMetricsReadRepository(ABC, BaseMetricsRepository):
    @abstractmethod
    async def get_metrics(self, query: MetricsQuery):
        ...


class MetricsWriteRepository(BaseMetricsWriteRepository):
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


class MetricsReadRepository(BaseMetricsReadRepository):
    async def get_metrics(self, query: MetricsQuery):
        where = [
            f"metric = '{query.metric}'",
            f"minute >= toDateTime('{query.from_ts:%Y-%m-%d %H:%M:%S}')",
            f"minute <= toDateTime('{query.to_ts:%Y-%m-%d %H:%M:%S}')",
        ]

        select_dims = ["minute"]
        group_by = ["minute"]

        if query.scope == "vm":
            where.append(f"host = '{query.host}'")
            where.append(f"vm = '{query.vm}'")
            select_dims += ["host", "vm"]
            group_by += ["host", "vm"]

        elif query.scope == "host":
            where.append(f"host = '{query.host}'")
            select_dims.append("host")
            group_by.append("host")

        sql = f"""
                SELECT
                    {", ".join(select_dims)},
                    sumMerge(sum_value) / countMerge(cnt_value) AS avg,
                    minMerge(min_value) AS min,
                    maxMerge(max_value) AS max
                FROM infra.metrics_1m
                WHERE {" AND ".join(where)}
                GROUP BY {", ".join(group_by)}
                ORDER BY minute
        """

        return await self.ch.fetch(sql)
