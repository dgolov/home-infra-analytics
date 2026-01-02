from aiochclient import ChClient
from fastapi import APIRouter, Depends
from typing import Dict

from core.db import MetricsRepository
from dependencies import get_ch_client
from src.schemas import MetricBatch

import logging


logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/test")
async def test() -> Dict[str, str]:
    """ Test endpoint
    """
    return {"test": "ok"}


@router.post("/metrics")
async def ingest(metrics: MetricBatch, ch_client: ChClient = Depends(get_ch_client)) -> Dict[str, str]:
    """ Add metric
    :param metrics:
    :param ch_client:
    :return:
    """
    repository = MetricsRepository(ch=ch_client)
    await repository.add_metric(data=metrics)
    return {"status": "ok"}
