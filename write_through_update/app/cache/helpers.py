from __future__ import annotations

import json
import logging
from typing import Any

from app.cache.redis_client import get_redis
from app.config import settings

logger = logging.getLogger("cache.helpers")


async def cache_get(key: str) -> Any | None:
    """Читает значение из кеша, возвращает распарсенный JSON или None."""
    r = await get_redis()
    raw = await r.get(key)
    if raw is None:
        logger.debug("[CACHE MISS] %s", key)
        return None
    logger.debug("[CACHE HIT]  %s", key)
    return json.loads(raw)


async def cache_set(key: str, value: Any, ttl: int | None = None) -> None:
    """Простой SET — используется при cache-aside (первичное наполнение)."""
    r = await get_redis()
    ttl = ttl or settings.base_ttl
    await r.set(key, json.dumps(value, default=str), ex=ttl)
    logger.debug("[CACHE SET]  %s  ttl=%ds", key, ttl)


async def cache_write_through(key: str, value: Any, ttl: int | None = None) -> None:
    """Write-Through SET — записываем в кеш СРАЗУ после записи в БД.

    Семантически то же, что cache_set, но помечаем отдельно для наблюдаемости.
    """
    r = await get_redis()
    ttl = ttl or settings.base_ttl
    await r.set(key, json.dumps(value, default=str), ex=ttl)
    logger.debug("[WRITE-THROUGH] %s  ttl=%ds", key, ttl)


async def cache_delete(key: str) -> int:
    """Удаление одного ключа (инвалидация)."""
    r = await get_redis()
    deleted = await r.delete(key)
    if deleted:
        logger.debug("[INVALIDATE] %s", key)
    return deleted


async def cache_delete_pattern(pattern: str) -> int:
    """Удаление по SCAN + паттерну. Возвращает количество удалённых."""
    r = await get_redis()
    count = 0
    async for key in r.scan_iter(match=pattern, count=200):
        await r.delete(key)
        logger.debug("[INVALIDATE] %s (pattern=%s)", key, pattern)
        count += 1
    return count


async def cache_delete_many(keys: list[str]) -> int:
    """Удаление списка ключей за один вызов."""
    if not keys:
        return 0
    r = await get_redis()
    deleted = await r.delete(*keys)
    for k in keys:
        logger.debug("[INVALIDATE] %s", k)
    return deleted
