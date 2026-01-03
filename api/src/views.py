from fastapi import APIRouter, Depends
from typing import Dict, List, Optional

from core.db import BaseMetricsReadRepository, BaseMetricsWriteRepository
from dependencies import get_read_repository, get_write_repository
from src.schemas import MetricBatch, MetricsQuery, LatestMetricsQuery, MetricsTopQuery, MetricsCardinalityQuery

import logging


logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/test")
async def test() -> Dict[str, str]:
    """ Test endpoint
    """
    return {"test": "ok"}


@router.get("/metrics")
async def query_metrics(
        query: MetricsQuery = Depends(),
        repository: BaseMetricsReadRepository = Depends(get_read_repository)
) -> List[Dict[str, str | float]]:
    """ Get metrics
    :param query:
    :param repository:
    :return:
    """
    return await repository.get_metrics(query)


@router.get("/metrics/latest")
async def latest_metrics(
    query: LatestMetricsQuery = Depends(),
    repository: BaseMetricsReadRepository = Depends(get_read_repository),
) -> Dict[str, str | float]:
    """ Get latest metrics
    :param query:
    :param repository:
    :return:
    """
    result: Optional[Dict[str, str | float]] = await repository.get_latest_metrics(query=query)
    if not result:
        return {"status": "no_data"}
    return result


@router.get("/metrics/top")
async def metrics_top(
    query: MetricsTopQuery = Depends(),
    repository: BaseMetricsReadRepository = Depends(get_read_repository),
) -> List[Dict[str, str | float]]:
    """ Get top metrics by host or vm
    :param query:
    :param repository:
    :return:
    """
    return await repository.get_top_metrics(query=query)


@router.get("/metrics/cardinality")
async def metrics_cardinality(
    query: MetricsCardinalityQuery = Depends(),
    repository: BaseMetricsReadRepository = Depends(get_read_repository),
) -> Dict[str, int]:
    """ Get cardinality metrics
    :param query:
    :param repository:
    :return:
    """
    return await repository.get_cardinality_metrics(query=query)


@router.post("/metrics")
async def ingest(
        metrics: MetricBatch,
        repository: BaseMetricsWriteRepository = Depends(get_write_repository)
) -> Dict[str, str]:
    """ Add metric
    :param metrics:
    :param repository:
    :return:
    """
    await repository.add_metric(data=metrics)
    return {"status": "ok"}
