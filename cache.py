import os
import redis
import redis.asyncio as aioredis

class MockRedisSync:
    def __init__(self):
        self.cache = {}
    
    def get(self, key):
        return self.cache.get(key)
    
    def setex(self, key, time, value):
        self.cache[key] = value
        return True
        
    def set(self, key, value):
        self.cache[key] = value
        return True

    def pipeline(self):
        return self
        
    def execute(self):
        return True

class MockRedisAsync:
    def __init__(self):
        self.cache = {}
    
    async def get(self, key):
        return self.cache.get(key)
    
    async def setex(self, key, time, value):
        self.cache[key] = value
        return True
        
    async def set(self, key, value):
        self.cache[key] = value
        return True

    async def mget(self, keys):
        return [self.cache.get(k) for k in keys]

redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")

# Synchronous Client (for background tasks & precompute)
try:
    redis_client_sync = redis.from_url(
        redis_url, 
        decode_responses=True,
        max_connections=50,
        socket_timeout=5.0
    )
    # Test connection
    redis_client_sync.ping()
    print(f"Connected to Redis (Sync) at {redis_url}")
except Exception as e:
    print(f"Redis (Sync) not available ({e}), using in-memory mock")
    redis_client_sync = MockRedisSync()

# Asynchronous Client (for API endpoints)
try:
    # aioredis.from_url is not awaitable in redis-py 4.2+, it returns client immediately
    redis_client_async = aioredis.from_url(
        redis_url, 
        decode_responses=True,
        max_connections=500,  # High concurrency
        socket_timeout=1.0,   # Fast fail
        socket_connect_timeout=1.0
    )
    print(f"Initialized Redis (Async) client for {redis_url}")
except Exception as e:
    print(f"Error initializing Redis (Async): {e}")
    redis_client_async = MockRedisAsync()
