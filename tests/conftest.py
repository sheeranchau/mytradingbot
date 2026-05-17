"""
Shared fixtures for tests.
"""
import asyncio
import pytest
import redis


def redis_available() -> bool:
    try:
        r = redis.Redis()
        r.ping()
        r.close()
        return True
    except Exception:
        return False


def zmq_available() -> bool:
    try:
        import zmq
        return True
    except ImportError:
        return False


requires_redis = pytest.mark.skipif(
    not redis_available(), reason="Redis not running"
)
requires_zmq = pytest.mark.skipif(
    not zmq_available(), reason="pyzmq not installed"
)


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def redis_store():
    """Provide a RedisStore connected to db=15 (test db), flushed before/after."""
    from core.redis_store import RedisStore
    store = RedisStore(url="redis://127.0.0.1:6379", db=15)
    await store.connect()
    await store.client.flushdb()
    yield store
    await store.client.flushdb()
    await store.close()
