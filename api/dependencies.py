from aiochclient import ChClient
from fastapi import Request

from core.db import MetricsReadRepository, MetricsWriteRepository


def get_ch_client(request: Request) -> ChClient:
    """ Get clickhouse client
    :param request:
    :return:
    """
    return request.app.state.ch_client


def get_write_repository(request: Request) -> MetricsWriteRepository:
    """ Get repository for write data to clickhouse
    :param request:
    :return:
    """
    return MetricsWriteRepository(ch=request.app.state.ch_client)


def get_read_repository(request: Request) -> MetricsReadRepository:
    """ Get repository for read data from clickhouse
    :param request:
    :return:
    """
    return MetricsReadRepository(ch=request.app.state.ch_client)
