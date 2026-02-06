from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from aiochclient import ChClient, Record

from core.redis import redis_cache
from src.helpers import calculate_delta, calculate_percents
from src.schemas import (
    CardinalityScope,
    LatestMetricsQuery,
    MetricBatch,
    MetricsBottomQuery,
    MetricsCardinalityQuery,
    MetricsCompareQuery,
    MetricsExtremesQuery,
    MetricsQuery,
    MetricsTopQuery,
    MetricsTrendQuery,
    Resolution,
)


class BaseMetricsRepository:
    def __init__(self, ch: ChClient):
        self.ch = ch


class BaseMetricsWriteRepository(ABC, BaseMetricsRepository):
    @abstractmethod
    async def add_metric(self, data: MetricBatch) -> None:
        ...


class BaseMetricsReadRepository(ABC, BaseMetricsRepository):
    @abstractmethod
    async def get_metrics(
            self,
            query: MetricsQuery
    ) -> List[Dict[str, str | float | datetime]]:
        ...

    @abstractmethod
    async def get_latest_metrics(
            self,
            query: LatestMetricsQuery
    ) -> Optional[Dict[str, str | float]]:
        ...

    @abstractmethod
    async def get_top_metrics(
            self,
            query: MetricsTopQuery
    ) -> List[Dict[str, str | float]]:
        ...

    @abstractmethod
    async def get_bottom_metrics(
            self,
            query: MetricsBottomQuery
    ) -> List[Dict[str, str | float]]:
        ...

    @abstractmethod
    async def get_extreme_metrics(
            self,
            query: MetricsExtremesQuery
    ) -> Dict[str, List[Dict[str, str | float]]]:
        ...

    @abstractmethod
    async def get_cardinality_metrics(
            self,
            query: MetricsCardinalityQuery
    ) -> Dict[str, int]:
        ...

    @abstractmethod
    async def get_compare_metrics(
            self,
            query: MetricsCompareQuery
    ) -> Dict[str, List[Dict[str, str | float]]]:
        ...

    @abstractmethod
    async def get_trend_metrics(
            self,
            query: MetricsTrendQuery
    ) -> Optional[Dict[str, float]]:
        ...


class MetricsWriteRepository(BaseMetricsWriteRepository):
    async def add_metric(self, data: MetricBatch) -> None:
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

    EXTREME_RULES = {
        "cpu_usage": "desc",
        "ram_used_pct": "asc",
        "disk_used_pct": "desc",
        "net_io": "desc",
    }

    ALLOWED_TABLES = {"infra.metrics_1m", "infra.metrics_5m", "infra.metrics_1h"}
    ALLOWED_BUCKETS = {"minute", "bucket"}

    @redis_cache(key_prefix="metrics", ttl=60)
    async def get_metrics(self, query: MetricsQuery) -> List[Dict[str, str | float | datetime]]:
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
                f"host = '{self._esc(query.host)}'",
                f"vm = '{self._esc(query.vm)}'",
            ]
            select_dims += ["host", "vm"]
            group_by += ["host", "vm"]

        elif query.scope == "host":
            where.append(f"host = '{self._esc(query.host)}'")
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
        """  # nosec B608 - ClickHouse SELECT does not support parameters

        result = await self.ch.fetch(sql)
        return list(dict(row) for row in result)

    @redis_cache(key_prefix="latest", ttl=60)
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
                f"host = '{self._esc(query.host)}'",
                f"vm = '{self._esc(query.vm)}'",
            ]
            select_dims += ["host", "vm"]
            group_by += ["host", "vm"]

        elif query.scope == "host":
            where.append(f"host = '{self._esc(query.host)}'")
            select_dims.append("host")
            group_by.append("host")

        subquery: str = f"""
            SELECT max({bucket})
            FROM {table}
            WHERE {" AND ".join(where)}
        """  # nosec B608 - ClickHouse SELECT does not support parameters

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
        """  # nosec B608 - ClickHouse SELECT does not support parameters

        rows: List[Record] = await self.ch.fetch(sql)
        return dict(rows[0]) if rows else None

    @redis_cache(key_prefix="top", ttl=60)
    async def get_top_metrics(self, query: MetricsTopQuery) -> List[Dict[str, str | float]]:
        """ get top metrics by host or vm
        :param query:
        :return:
        """
        table, bucket = self.__get_table_and_bucket(resolution=query.resolution)
        limit: int = self._safe_limit(value=query.limit)

        where = [f"metric = '{query.metric}'"]

        if query.scope == "vm":
            group_by = ["host", "vm"]
            select_dims = ["host", "vm"]
            if query.host:
                where.append(f"host = '{self._esc(query.host)}'")

        elif query.scope == "host":
            group_by = ["host"]
            select_dims = ["host"]

        else:
            raise ValueError("scope must be vm or host")

        last_bucket_sql = f"""
                SELECT max({bucket})
                FROM {table}
                WHERE {" AND ".join(where)}
            """  # nosec B608 - ClickHouse SELECT does not support parameters

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
            LIMIT {limit}
            """  # nosec B608 - ClickHouse SELECT does not support parameters

        result = await self.ch.fetch(sql)
        return list(dict(row) for row in result)

    @redis_cache(key_prefix="bottom", ttl=60)
    async def get_bottom_metrics(self, query: MetricsBottomQuery) -> List[Dict[str, str | float]]:
        """ Get bottom metrics
        :param query:
        :return:
        """
        table, bucket = self.__get_table_and_bucket(resolution=query.resolution)
        limit: int = self._safe_limit(value=query.limit)

        where: List[str] = [f"metric = '{self._esc(query.metric)}'"]

        if query.scope == "vm":
            entity = "vm"
            if query.host:
                where.append(f"host = '{self._esc(query.host)}'")

        elif query.scope == "host":
            entity = "host"

        else:
            raise ValueError("scope must be vm or host")

        sql: str = f"""
            SELECT
                {entity} AS name,
                avgMerge(avg_value) AS value
            FROM {table}
            WHERE {" AND ".join(where)}
            GROUP BY name
            ORDER BY value ASC
            LIMIT {limit}
        """  # nosec B608 - ClickHouse SELECT does not support parameters

        result: List[Record] = await self.ch.fetch(sql)
        return list(dict(row) for row in result)

    @redis_cache(key_prefix="extreme", ttl=60)
    async def get_extreme_metrics(self, query: MetricsExtremesQuery) -> Dict[str, Any]:
        """ get extreme metrics
        :param query:
        :return:
        """
        table, bucket = self.__get_table_and_bucket(query.resolution)

        sql: str = f"""
                SELECT
                    vm,
                    metric,
                    avgMerge(avg_value) AS value
                FROM {table}
                WHERE
                    metric IN {tuple(self.EXTREME_RULES.keys())}
                    AND {bucket} >= toDateTime('{query.from_ts:%Y-%m-%d %H:%M:%S}')
                    AND {bucket} <= toDateTime('{query.to_ts:%Y-%m-%d %H:%M:%S}')
                GROUP BY
                    vm,
                    metric
            """  # nosec B608 - ClickHouse SELECT does not support parameters

        rows: List[Record] = await self.ch.fetch(sql)
        if not rows:
            return {}

        return self._sort_extremes(rows, query.limit)

    @redis_cache(key_prefix="cardinality", ttl=60)
    async def get_cardinality_metrics(self, query: MetricsCardinalityQuery) -> Dict[str, int]:
        """ get cardinality metrics
        :param query:
        :return:
        """
        table, bucket = self.__get_table_and_bucket(resolution=query.resolution)

        if query.scope == CardinalityScope.vm:
            uniq_expr = "uniq(vm)"
        elif query.scope == CardinalityScope.host:
            uniq_expr = "uniq(host)"
        elif query.scope == CardinalityScope.metric:
            uniq_expr = "uniq(metric)"
        else:
            raise ValueError("Invalid scope")

        where: List[str] = []

        if query.from_ts and query.to_ts:
            where += [
                f"{bucket} >= toDateTime('{query.from_ts:%Y-%m-%d %H:%M:%S}')",
                f"{bucket} <= toDateTime('{query.to_ts:%Y-%m-%d %H:%M:%S}')"
            ]
        else:
            where.append(
                f"{bucket} = (SELECT max({bucket}) FROM {table})"   # nosec B608 - ClickHouse SELECT does not support parameters
            )

        sql: str = f"""
            SELECT
                {uniq_expr} AS count
            FROM {table}
            WHERE {" AND ".join(where)}
        """  # nosec B608 - ClickHouse SELECT does not support parameters

        rows: List[Record] = await self.ch.fetch(sql)
        return dict(rows[0]) if rows else {"count": 0}

    @redis_cache(key_prefix="compare", ttl=60)
    async def get_compare_metrics(self, query: MetricsCompareQuery) -> Dict[str, Any]:
        """ get compare metrics by before period and after period
        :param query:
        :return:
        """
        table, bucket = self.__get_table_and_bucket(resolution=query.resolution)

        where: List[str] = [f"metric = '{self._esc(query.metric)}'"]

        if query.scope == "vm":
            where += [
                f"host = '{self._esc(query.host)}'",
                f"vm = '{self._esc(query.vm)}'",
            ]
        elif query.scope == "host":
            where.append(f"host = '{self._esc(query.host)}'")

        after: Optional[Record] = await self._aggregate_period(
            table=table,
            bucket=bucket,
            where=where,
            from_ts=query.from_a,
            to_ts=query.to_a
        )
        before: Optional[Record] = await self._aggregate_period(
            table=table,
            bucket=bucket,
            where=where,
            from_ts=query.from_b,
            to_ts=query.to_b
        )

        if not after or not before:
            return {"status": "no_data"}

        return {
            "before": dict(after),
            "after": dict(before),
            "delta": {
                "avg": calculate_delta(after["avg"], before["avg"]),
                "min": calculate_delta(after["min"], before["min"]),
                "max": calculate_delta(after["max"], before["max"]),
            },
            "delta_percent": {
                "avg": calculate_percents(after["avg"], before["avg"]),
                "min": calculate_percents(after["min"], before["min"]),
                "max": calculate_percents(after["max"], before["max"]),
            },
        }

    @redis_cache(key_prefix="trend", ttl=60)
    async def get_trend_metrics(self, query: MetricsTrendQuery) -> Optional[Dict[str, float]]:
        """
        :param query:
        :return:
        """
        table, bucket = self.__get_table_and_bucket(resolution=query.resolution)

        where: List[str] = [f"metric = '{self._esc(query.metric)}'"]

        if query.scope == "vm":
            where += [
                f"host = '{self._esc(query.host)}'",
                f"vm = '{self._esc(query.vm)}'",
            ]
        elif query.scope == "host":
            where.append(f"host = '{self._esc(query.host)}'")

        sql = f"""
            SELECT
                {bucket} AS ts,
                avgMerge(avg_value) AS avg_value
            FROM {table}
            WHERE
                {" AND ".join(where)}
                AND {bucket} >= toDateTime('{query.from_ts:%Y-%m-%d %H:%M:%S}')
                AND {bucket} <= toDateTime('{query.to_ts:%Y-%m-%d %H:%M:%S}')
            GROUP BY ts
            ORDER BY ts
        """  # nosec B608 - ClickHouse SELECT does not support parameters

        rows: List[Record] = await self.ch.fetch(sql)
        if not rows:
            return None

        ts_list = [r["ts"].timestamp() for r in rows]
        avg_list = [r["avg_value"] for r in rows]

        slope, intercept = self.__linear_regression(ts_list, avg_list)

        return {"slope": slope, "intercept": intercept}

    async def _aggregate_period(
            self,
            table: str,
            bucket: str,
            where: list[str],
            from_ts: datetime,
            to_ts: datetime,
    ) -> Optional[Record]:
        sql: str = f"""
            SELECT
                sumMerge(sum_value) / countMerge(cnt_value) AS avg,
                minMerge(min_value) AS min,
                maxMerge(max_value) AS max
            FROM {table}
            WHERE
                {" AND ".join(where)}
                AND {bucket} >= toDateTime('{from_ts:%Y-%m-%d %H:%M:%S}')
                AND {bucket} <= toDateTime('{to_ts:%Y-%m-%d %H:%M:%S}')
        """  # nosec B608 - ClickHouse SELECT does not support parameters
        rows: List[Record] = await self.ch.fetch(sql)
        return rows[0] if rows else None

    def _sort_extremes(
            self,
            rows: List[Record],
            limit: int,
    ) -> Dict[str, List[Dict[str, float]]]:
        """ sort results for extremes metrics by EXTREME_RULES
        :param rows:
        :param limit:
        :return:
        """
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

        for row in rows:
            grouped[row["metric"]].append({
                "vm": row["vm"],
                "value": float(row["value"]),
            })

        result: Dict[str, List[Dict[str, Any]]] = {}

        for metric, items in grouped.items():
            order = self.EXTREME_RULES.get(metric, "desc")
            reverse = order == "desc"

            sorted_items = sorted(
                items,
                key=lambda item: item["value"],
                reverse=reverse,
            )

            result[metric] = sorted_items[:limit]

        return result

    @staticmethod
    def _esc(value: str | None) -> str | None:
        """ Escape special characters
        """
        if not value:
            return None
        return value.replace("\\", "\\\\").replace("'", "\\'")

    @staticmethod
    def _safe_limit(value: int, *, max_limit: int = 1000) -> int:
        """ Safe limit
        """
        if not isinstance(value, int):
            raise ValueError("limit must be int")

        if value <= 0:
            raise ValueError("limit must be positive")

        return min(value, max_limit)

    def __get_table_and_bucket(self, resolution: str) -> Tuple[str, str]:
        """ get item table and time bucket
        :param resolution:
        :return:
        """
        table: str = self.TABLE_BY_RESOLUTION[resolution]
        bucket: str = "minute" if resolution == Resolution.m1 else "bucket"

        if table not in self.ALLOWED_TABLES:
            raise ValueError(f"Invalid table name: {table}")

        if bucket not in self.ALLOWED_BUCKETS:
            raise ValueError(f"Invalid bucket name: {bucket}")

        return table, bucket

    @staticmethod
    def __linear_regression(ts_list: list[float], avg_list: list[float]) -> tuple[float, float]:
        """ get slope and intercept for trend metrics
        :param ts_list:
        :param avg_list:
        :return:
        """
        ts_len = len(ts_list)
        if ts_len == 0:
            return 0.0, 0.0
        if ts_len == 1:
            return 0.0, avg_list[0]

        sum_ts_list = sum(ts_list)
        sum_avg_list = sum(avg_list)
        sum_xx = sum(v * v for v in ts_list)
        sum_xy = sum(v * u for v, u in zip(ts_list, avg_list, strict=True))

        slope = (ts_len * sum_xy - sum_ts_list * sum_avg_list) / (ts_len * sum_xx - sum_ts_list * sum_ts_list)
        intercept = (sum_avg_list - slope * sum_ts_list) / ts_len
        return slope, intercept
