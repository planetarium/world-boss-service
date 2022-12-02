import os
from datetime import timedelta

from redis import StrictRedis

__all__ = [
    'cache_exists',
    'set_to_cache',
]

rd = StrictRedis(host=os.environ['REDIS_HOST'], port=int(os.environ['REDIS_PORT']), db=0)


def cache_exists(key: str):
    return rd.exists(key)


def set_to_cache(key: str, pickled_object, ttl: timedelta = timedelta(minutes=60)):
    rd.setex(key, ttl, pickled_object)
