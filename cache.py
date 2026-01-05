import os
import redis
import redis.asyncio as redis_async

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

# Sync client → startup / precompute
redis_sync = redis.Redis.from_url(
    REDIS_URL,
    decode_responses=False,
)

# Async client → API serving
redis_async_client = redis_async.from_url(
    REDIS_URL,
    decode_responses=False,
)
