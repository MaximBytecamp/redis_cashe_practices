from __future__ import annotations

import asyncio
import time
import uuid


# ── Однонодовый алгоритм 

class RedisLock:
    """
    Распределённый лок на одной Redis ноде.

    Ключевые свойства:
      1. Atomic SET NX — нельзя взять дважды
      2. TTL — авто-освобождение при краше процесса
      3. Token — нельзя освободить чужой лок (защита от race condition)
    """

    LUA_RELEASE = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        else
            return 0
        end
    """

    def __init__(self, redis_client, key: str, ttl_ms: int = 5000):
        self.redis = redis_client
        self.key = key
        self.ttl_ms = ttl_ms
        self.token: str | None = None

    async def acquire(self) -> bool:
        """
        Попытка захватить лок.
        Возвращает True если успешно, False если лок уже занят.
        """
        self.token = str(uuid.uuid4())  # уникальный токен

        result = await self.redis.set(
            self.key,
            self.token,
            nx=True,          # Only if Not eXists
            px=self.ttl_ms,   # TTL в миллисекундах
        )
        return result is not None

    async def release(self) -> bool:
        """
        Освободить лок ТОЛЬКО если он наш (сравниваем токен).
        Атомарно через Lua — между GET и DEL не может вклиниться другой процесс.
        """
        if self.token is None:
            return False
        result = await self.redis.eval(
            self.LUA_RELEASE,
            1,             # количество ключей
            self.key,      # KEYS[1]
            self.token,    # ARGV[1]
        )
        self.token = None
        return bool(result)

    async def extend(self, extra_ms: int) -> bool:
        """Продлить TTL пока держим лок (для долгих операций)."""
        if self.token is None:
            return False
        script = """
            if redis.call("GET", KEYS[1]) == ARGV[1] then
                return redis.call("PEXPIRE", KEYS[1], ARGV[2])
            else
                return 0
            end
        """
        result = await self.redis.eval(script, 1, self.key, self.token, extra_ms)
        return bool(result)


# ── Использование как async context manager 

class LockContext:
    """
    Паттерн использования:

        async with LockContext(redis, "product:1:lock", ttl_ms=3000) as acquired:
            if acquired:
                data = await db.get(product_id)
                await cache.set(product_id, data)
            else:
                await asyncio.sleep(0.05)  # retry
    """

    def __init__(self, redis_client, key: str, ttl_ms: int = 5000):
        self.lock = RedisLock(redis_client, key, ttl_ms)

    async def __aenter__(self) -> bool:
        return await self.lock.acquire()

    async def __aexit__(self, *args) -> None:
        await self.lock.release()


# ── Retry loop — классический паттерн 

async def with_lock_retry(redis, key: str, work_fn, ttl_ms=3000,
                          max_retries=10, retry_delay=0.1):
    """
    Попытаться взять лок с повторами.

    Алгоритм:
      1. Попытка взять лок
      2. Если не получилось → подождать random(0, retry_delay) → повтор
      3. Если взяли → выполнить работу → освободить лок
    """
    import random

    lock = RedisLock(redis, key, ttl_ms)

    for attempt in range(max_retries):
        if await lock.acquire():
            try:
                return await work_fn()
            finally:
                await lock.release()

        # Jitter чтобы не все ретраились одновременно
        wait = random.uniform(0, retry_delay)
        await asyncio.sleep(wait)

    raise TimeoutError(f"Could not acquire lock '{key}' after {max_retries} attempts")


# ── Redlock алгоритм (5 нод) 

class Redlock:
    """
    Алгоритм Redlock от автора Redis (Martin Kleppmann спорил, Antirez настаивал).

    Суть: взять лок на большинстве нод (3 из 5).
    Если не успели взять за половину TTL — откат.
    """

    def __init__(self, redis_nodes: list, ttl_ms: int = 5000):
        self.nodes = redis_nodes
        self.ttl_ms = ttl_ms
        self.quorum = len(redis_nodes) // 2 + 1
        self.clock_drift_ms = 10  # допуск на расхождение часов

    async def acquire(self, key: str) -> tuple[bool, str]:
        token = str(uuid.uuid4())
        start = time.monotonic()
        acquired_count = 0

        for node in self.nodes:
            try:
                ok = await node.set(key, token, nx=True, px=self.ttl_ms)
                if ok:
                    acquired_count += 1
            except Exception:
                pass  # нода недоступна — идём дальше

        elapsed_ms = (time.monotonic() - start) * 1000
        validity_ms = self.ttl_ms - elapsed_ms - self.clock_drift_ms

        if acquired_count >= self.quorum and validity_ms > 0:
            return True, token

        # Не получилось — освободить на всех нодах
        await self._release_all(key, token)
        return False, ""

    async def _release_all(self, key: str, token: str) -> None:
        lua = RedisLock.LUA_RELEASE
        for node in self.nodes:
            try:
                await node.eval(lua, 1, key, token)
            except Exception:
                pass


