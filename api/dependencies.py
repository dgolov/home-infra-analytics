from aiochclient import ChClient
from fastapi import Request

from core.db import MetricsReadRepository, MetricsWriteRepository


def get_ch_client(request: Request) -> ChClient:
    return request.app.state.ch_client


def get_write_repository(request: Request) -> MetricsWriteRepository:
    return MetricsWriteRepository(ch=request.app.state.ch_client)


def get_read_repository(request: Request) -> MetricsReadRepository:
    return MetricsReadRepository(ch=request.app.state.ch_client)
