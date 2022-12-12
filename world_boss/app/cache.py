import os
from datetime import timedelta
from typing import Union, cast

from redis import StrictRedis

__all__ = [
    "cache_exists",
    "get_from_cache",
    "set_to_cache",
]

from world_boss.app.config import config

rd = StrictRedis(host=config.redis_host, port=config.redis_port, db=0)


def cache_exists(key: str):
    return rd.exists(key)


def set_to_cache(key: str, pickled_object, ttl: timedelta = timedelta(minutes=60)):
    rd.setex(key, ttl, pickled_object)


def get_from_cache(key: str) -> Union[str, bytes]:
    return cast(Union[str, bytes], rd.get(key))
