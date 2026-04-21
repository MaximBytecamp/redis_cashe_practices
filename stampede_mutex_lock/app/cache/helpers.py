"""Кеш-хелперы + mutex lock операции.

Кеш:
  cache_get(key)    → dict | None
  cache_set(key, v) → None
  cache_delete(key) → int

Lock (с owner token для безопасного освобождения):
  lock_acquire(key, owner)  → bool
  lock_release(key, owner)  → bool
  lock_exists(key)          → bool

Логирование:
  [CACHE HIT]        product:1
  [CACHE MISS]       product:1
  [CACHE SET]        product:1  ttl=120s
  [CACHE DELETE]     product:1
  [LOCK ACQUIRED]    lock:product:1  owner=abc123  ttl=5s
  [LOCK BUSY]        lock:product:1  (owner=xyz789 holds it)
  [LOCK RELEASED]    lock:product:1  owner=abc123
  [LOCK RELEASE SKIP] lock:product:1  owner mismatch
"""

from __future__ import annotations

import json
import logging
from typing import Any

from app.cache.redis_client import get_redis
from app.config import settings

logger = logging.getLogger("cache")


# ─── Кеш-операции

async def cache_get(key: str) -> dict | None:
    r = await get_redis()
    raw = await r.get(key)

    if raw is None:
        logger.info("[CACHE MISS]       %s", key)
        return None

    logger.info("[CACHE HIT]        %s", key)
    return json.loads(raw)


async def cache_set(key: str, value: Any, ttl: int | None = None) -> None:
    """Записать данные в кеш с TTL."""
    r = await get_redis()
    ttl = ttl or settings.product_cache_ttl
    await r.set(key, json.dumps(value, default=str), ex=ttl)
    logger.info("[CACHE SET]        %s  ttl=%ds", key, ttl)


async def cache_delete(key: str) -> int:
    """Удалить ключ из кеша."""
    r = await get_redis()
    deleted = await r.delete(key)
    if deleted:
        logger.info("[CACHE DELETE]     %s", key)
    return deleted


# ─── Lock-операции (с owner token)

# Lua-скрипт: удалить lock ТОЛЬКО если owner совпадает.
# Это предотвращает удаление чужого lock-а при гонке.
_RELEASE_LUA = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""

# Суть проблемы: не в том, что Python «плохой», а в том, что GET и DEL — это два отдельных сетевых запроса к Redis.
# Где гонка: между ними есть окно времени, где другой воркер может изменить lock.
# Что даёт Lua в Redis: EVAL выполняет весь скрипт внутри Redis как одну атомарную секцию (без вклинивания других команд между get и del).
# Итог: check owner + delete делается как единая операция без разрыва.

async def lock_acquire(lock_key: str, owner: str, ttl: int | None = None) -> bool:
    r = await get_redis()
    ttl = ttl or settings.lock_ttl_seconds
    acquired = await r.set(lock_key, owner, nx=True, ex=ttl)

    if acquired:
        logger.info("[LOCK ACQUIRED]    %s  owner=%s  ttl=%ds", lock_key, owner, ttl)
        return True

    # lock занят — показать кто владеет
    current_owner = await r.get(lock_key)
    logger.info("[LOCK BUSY]        %s  (owner=%s holds it)", lock_key, current_owner)
    return False


async def lock_release(lock_key: str, owner: str) -> bool:
    r = await get_redis()
    result = await r.eval(_RELEASE_LUA, 1, lock_key, owner)

    if result:
        logger.info("[LOCK RELEASED]    %s  owner=%s", lock_key, owner)
        return True

    logger.warning("[LOCK RELEASE SKIP] %s  owner=%s mismatch", lock_key, owner)
    return False


async def lock_exists(lock_key: str) -> bool:
    """Проверить, существует ли lock-ключ."""
    r = await get_redis()
    return bool(await r.exists(lock_key))
