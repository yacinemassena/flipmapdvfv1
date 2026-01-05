# cache.py
import os
import redis.asyncio as aioredis

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

redis_client = aioredis.from_url(
    REDIS_URL,
    decode_responses=True,
    max_connections=500,
    socket_timeout=1.0,
    socket_connect_timeout=1.0,
)
