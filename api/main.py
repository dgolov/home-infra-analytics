import logging
from typing import Awaitable, Callable

from aiochclient import ChClient
from aiohttp import ClientSession
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from config import settings, setup_logging
from core.redis import connect_to_redis
from src.views import router

setup_logging(log_level=settings.log_level, log_file=settings.log_path)
logger = logging.getLogger(__name__)

app = FastAPI()
app.state.http_session = None
app.state.ch_client = None
app.include_router(router)


@app.on_event("startup")
async def startup() -> None:
    app.state.http_session = ClientSession()
    app.state.ch_client = ChClient(
        app.state.http_session,
        url=settings.clickhouse_url,
        user=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_db
    )
    await connect_to_redis(
        host=settings.redis_host,
        port=settings.redis_port,
        password=settings.redis_password
    )


@app.on_event("shutdown")
async def shutdown() -> None:
    await app.state.http_session.close()


@app.middleware("http")
async def requests_middleware(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    """ logging http requests and errors
    :param request:
    :param call_next:
    :return:
    """
    try:
        response = await call_next(request)
        params = request.scope['query_string'].decode()
        source_url = f"{request.scope['client'][0]}:{request.scope['client'][1]}"
        if params:
            params = f"?{params}"
        logger.debug(
            f"{source_url} - '{request.method} {request.scope['path'] + params} "
            f"{request.scope['scheme']}/{request.scope['http_version']} {response.status_code}'"
        )
        return response
    except Exception as e:
        logger.error(str(e))
        return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})
