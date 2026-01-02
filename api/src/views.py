from clickhouse_driver import Client
from fastapi import APIRouter, Depends

from core.db import MetricsRepository
from dependencies import get_clickhouse_client
from src.schemas import Metric


router = APIRouter()


@router.get("/test")
async def test() -> dict:
    """ Test endpoint
    """
    return {"test": "ok"}


@router.post("/metrics")
def ingest(metric: Metric, client: Client = Depends(get_clickhouse_client),):
    """ Add metric
    :param metric:
    :param client:
    :return:
    """
    repository = MetricsRepository()
    repository.add_metric(client=client, metric=metric)
    return {"status": "ok"}
