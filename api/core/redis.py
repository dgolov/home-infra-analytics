from datetime import datetime
from typing import Any, Dict, Optional, Coroutine, TypeVar, Callable, ParamSpec

from src.helpers import json_serializer

import functools
import logging
import json
import redis.asyncio as redis


P = ParamSpec("P")
R = TypeVar("R")

logger = logging.getLogger(__name__)

redis_client = None


async def connect_to_redis(host: str, port: str | int, password: str) -> None:
    """ Connect to redis
    :param host:
    :param port:
    :param password:
    :return:
    """
    global redis_client
    logger.debug("Try connect to Redis")

    try:
        client = redis.from_url(  # type: ignore[no-untyped-call]
            url=f"redis://:{password}@{host}:{port}",
            encoding="utf-8",
            decode_responses=True
        )
        await client.ping()
        logger.info("Connect to redis successfully")
    except redis.AuthenticationError:
        logger.error("Redis authentication failed! Wrong password?")
        raise
    except redis.ConnectionError:
        logger.error("Cannot connect to Redis server!")
        raise
    except Exception as e:
        logger.error(f"Unexpected error connecting to Redis: {e}")
        raise
    else:
        redis_client = client


async def get_cache(key: str) -> Optional[Dict[str, Any]]:
    """ Get data from cache by key
    :param key:
    :return:
    """
    if not redis_client:
        return None
    data = await redis_client.get(key)
    if data:
        return json.loads(data) # type: ignore[no-any-return]
    return None


async def set_cache(key: str, value: Dict[str, Any], ttl: int = 60) -> None:
    """ Set data to cache
    :param key:
    :param value:
    :param ttl:
    :return:
    """
    if not redis_client:
        return
    await redis_client.set(key, json.dumps(value, default=json_serializer), ex=ttl)


def make_cache_key(
        key: str,
        metric: str,
        scope: Optional[str] = None,
        host: Optional[str] = None,
        vm: Optional[str] = None,
        from_ts: Optional[str] = None,
        to_ts: Optional[str] = None,
        from_a: Optional[str] = None,
        to_a: Optional[str] = None,
        from_b: Optional[str] = None,
        to_b: Optional[str] = None,
        resolution: Optional[str] = None
) -> str:
    """ Make redis cache key
    :param key:
    :param metric:
    :param scope:
    :param host:
    :param vm:
    :param from_ts:
    :param to_ts:
    :param from_a:
    :param to_a:
    :param from_b:
    :param to_b:
    :param resolution:
    :return:
    """
    cache_key: str = f"{key}:{metric}"
    if scope:
        cache_key += f":{scope}"
    if host:
        cache_key += f":{host}"
    if vm:
        cache_key += f":{vm}"
    if from_ts:
        cache_key += f":{from_ts}"
    if to_ts:
        cache_key += f":{to_ts}"
    if from_a:
        cache_key += f":{from_a}"
    if to_a:
        cache_key += f":{to_a}"
    if from_b:
        cache_key += f":{from_b}"
    if to_b:
        cache_key += f":{to_b}"
    if resolution:
        cache_key += f":{resolution}"
    return cache_key


def redis_cache(key_prefix: str, ttl: int = 60) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """ Cache decorator
    :param key_prefix:
    :param ttl:
    :return:
    """
    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        async def wrapper(self: Any, *args: P.args, **kwargs: P.kwargs) -> Dict[str, Any]:
            query: Any = kwargs.get("query") or (args[0] if args else None)
            if query is None:
                raise ValueError("Decorator expects a 'query' argument")

            cache_key: Optional[str] = None

            if redis_client:
                cache_key = make_cache_key(
                    key=key_prefix,
                    metric=getattr(query, "metric", "unknown"),
                    scope=getattr(query, "scope", None),
                    host=getattr(query, "host", None),
                    vm=getattr(query, "vm", None),
                    from_ts=iso(getattr(query, "from_ts", None)),
                    to_ts=iso(getattr(query, "to_ts", None)),
                    from_a=iso(getattr(query, "from_a", None)),
                    to_a=iso(getattr(query, "to_a", None)),
                    resolution=getattr(query, "resolution", None)
                )

                cached: Optional[Dict[str, Any]] = await get_cache(cache_key)
                if cached:
                    return cached

            result: Dict[str, Any] = await func(self, *args, **kwargs)  # type: ignore

            if cache_key:
                if result is not None:
                    await set_cache(cache_key, result, ttl=ttl)

            return result

        return wrapper  # type: ignore
    return decorator


def iso(dt: Optional[datetime]) -> str:
    return dt.isoformat() if dt else ""
