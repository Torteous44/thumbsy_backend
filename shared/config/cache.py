# shared/config/cache.py
import os
import redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def ping_cache():
    try:
        return redis_client.ping()
    except Exception as e:
        print(f"Redis connection error: {e}")
        return False
