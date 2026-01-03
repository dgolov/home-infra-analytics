from abc import ABC, abstractmethod
from aiochclient import ChClient, Record
from datetime import datetime
from typing import Tuple, Dict, List, Optional, Any

from src.schemas import MetricBatch, MetricsQuery, LatestMetricsQuery, MetricsTopQuery, Resolution


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

    @abstractmethod
    async def get_latest_metrics(self, query: LatestMetricsQuery) -> Optional[Dict[str, str | float]]:
        ...

    @abstractmethod
    async def get_top_metrics(self, query: MetricsTopQuery):
        ...


class MetricsWriteRepository(BaseMetricsWriteRepository):
    async def add_metric(self, data: MetricBatch) -> List[Record]:
        """ insert metric batch
        :param data:
        :return:
        """
        now: datetime = datetime.utcnow()

        rows: List[Dict[str, Any]] = [
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

    async def get_metrics(self, query: MetricsQuery) -> List[Record]:
        """ Get metrics
        :param query:
        :return:
        """
        table, bucket = self.__get_table_and_bucket(resolution=query.resolution)

        where: List[str] = [
            f"metric = '{query.metric}'",
            f"{bucket} >= toDateTime('{query.from_ts:%Y-%m-%d %H:%M:%S}')",
            f"{bucket} <= toDateTime('{query.to_ts:%Y-%m-%d %H:%M:%S}')",
        ]

        select_dims: List[str] = [bucket]
        group_by: List[str] = [bucket]

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

        sql: str = f"""
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

    async def get_latest_metrics(self, query: LatestMetricsQuery) -> Optional[Dict[str, str | float]]:
        """ Get latest metrics
        :param query:
        :return:
        """
        table, bucket = self.__get_table_and_bucket(resolution=query.resolution)

        where: List[str] = [f"metric = '{query.metric}'"]

        select_dims: List[str] = []
        group_by: List[str] = []

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

        subquery: str = f"""
            SELECT max({bucket})
            FROM {table}
            WHERE {" AND ".join(where)}
        """

        sql: str = f"""
            SELECT
                {", ".join(select_dims + [bucket])},
                sumMerge(sum_value) / countMerge(cnt_value) AS avg,
                minMerge(min_value) AS min,
                maxMerge(max_value) AS max
            FROM {table}
            WHERE
                {" AND ".join(where)}
                AND {bucket} = ({subquery})
            GROUP BY
                {", ".join(group_by + [bucket])}
        """

        rows: List[Record] = await self.ch.fetch(sql)
        return rows[0] if rows else None

    async def get_top_metrics(self, query: MetricsTopQuery) -> List[Record]:
        """ get top metrics by host or vm
        :param query:
        :return:
        """
        table, bucket = self.__get_table_and_bucket(resolution=query.resolution)

        where = [f"metric = '{query.metric}'"]

        if query.scope == "vm":
            group_by = ["host", "vm"]
            select_dims = ["host", "vm"]
            if query.host:
                where.append(f"host = '{query.host}'")

        elif query.scope == "host":
            group_by = ["host"]
            select_dims = ["host"]

        else:
            raise ValueError("scope must be vm or host")

        last_bucket_sql = f"""
                SELECT max({bucket})
                FROM {table}
                WHERE {" AND ".join(where)}
            """

        sql = f"""
            SELECT
                {", ".join(select_dims)},
                sumMerge(sum_value) / countMerge(cnt_value) AS avg,
                minMerge(min_value) AS min,
                maxMerge(max_value) AS max
            FROM {table}
            WHERE
                {" AND ".join(where)}
                AND {bucket} = ({last_bucket_sql})
            GROUP BY
                {", ".join(group_by)}
            ORDER BY avg DESC
            LIMIT {query.limit}
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
