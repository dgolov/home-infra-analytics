from fastapi import APIRouter, Depends
from typing import Dict, List, Optional

from core.db import BaseMetricsReadRepository, BaseMetricsWriteRepository
from dependencies import get_read_repository, get_write_repository
from src.schemas import MetricBatch, MetricsQuery, LatestMetricsQuery

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
    repo: BaseMetricsReadRepository = Depends(get_read_repository),
) -> Dict[str, str | float]:
    """ Get latest metrics
    :param query:
    :param repo:
    :return:
    """
    result: Optional[Dict[str, str | float]] = await repo.get_latest_metrics(query)
    if not result:
        return {"status": "no_data"}
    return result


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
