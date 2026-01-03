from abc import ABC, abstractmethod
from aiochclient import ChClient
from datetime import datetime
from typing import Tuple, Dict

from src.schemas import MetricBatch, MetricsQuery, Resolution


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
        """ insert metric batch
        :param data:
        :return:
        """
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
    TABLE_BY_RESOLUTION: Dict[str, str] = {
        Resolution.m1: "infra.metrics_1m",
        Resolution.m5: "infra.metrics_5m",
        Resolution.h1: "infra.metrics_1h",
    }

    async def get_metrics(self, query: MetricsQuery):
        """ Get metrics
        :param query:
        :return:
        """
        table, bucket = self.__get_table_and_bucket(resolution=query.resolution)

        where = [
            f"metric = '{query.metric}'",
            f"{bucket} >= toDateTime('{query.from_ts:%Y-%m-%d %H:%M:%S}')",
            f"{bucket} <= toDateTime('{query.to_ts:%Y-%m-%d %H:%M:%S}')",
        ]

        select_dims = [bucket]
        group_by = [bucket]

        if query.scope == "vm":
            where += [
                f"host = '{query.host}'",
                f"vm = '{query.vm}'",
            ]
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
                FROM {table}
                WHERE {" AND ".join(where)}
                GROUP BY {", ".join(group_by)}
                ORDER BY {bucket}
        """

        return await self.ch.fetch(sql)

    def __get_table_and_bucket(self, resolution: str) -> Tuple[str, str]:
        """ get item table and time bucket
        :param resolution:
        :return:
        """
        table: str = self.TABLE_BY_RESOLUTION[resolution]
        bucket: str = "minute" if resolution == Resolution.m1 else "bucket"
        return table, bucket
