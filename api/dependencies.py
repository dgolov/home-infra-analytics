from aiochclient import ChClient
from fastapi import Request


def get_ch_client(request: Request) -> ChClient:
    return request.app.state.ch_client
