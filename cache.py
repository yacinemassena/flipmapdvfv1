import os
import redis

class MockRedis:
    def __init__(self):
        self.cache = {}
    
    def get(self, key):
        return self.cache.get(key)
    
    def setex(self, key, time, value):
        self.cache[key] = value
        
    def set(self, key, value):
        self.cache[key] = value

try:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    redis_client = redis.from_url(redis_url, decode_responses=True)
    # Test connection
    redis_client.ping()
    print(f"Connected to Redis at {redis_url}")
except Exception as e:
    print(f"Redis not available ({e}), using in-memory mock")
    redis_client = MockRedis()
