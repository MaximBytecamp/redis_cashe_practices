"""
Высокоуровневые хелперы для работы с кэшем.

Вся логика чтения/записи/инвалидации кэша — здесь.
"""

from __future__ import annotations

import json
import logging
import random
from typing import Any

from app.cache.redis_client import get_redis
from app.config import settings

logger = logging.getLogger("cache")


# TTL с jitter (страховка)


def get_ttl() -> int:
    """TTL = base + random(0, jitter). Страховка на случай пропущенного invalidate."""
    return settings.base_ttl + random.randint(0, settings.jitter_max)


# Cache Read / Write

async def cache_get(key: str) -> Any | None:
    """Прочитать из кэша. Возвращает десериализованные данные или None."""
    r = await get_redis()
    raw = await r.get(key)
    if raw is None:
        logger.info("CACHE MISS  %s", key)
        return None
    logger.info("CACHE HIT   %s", key)
    return json.loads(raw)


async def cache_set(key: str, data: Any) -> None:
    """Записать в кэш с TTL + jitter."""
    r = await get_redis()
    ttl = get_ttl()
    await r.set(key, json.dumps(data, default=str), ex=ttl)
    logger.info("CACHE SET   %s  ttl=%d", key, ttl)


# Cache Invalidate

async def cache_delete(key: str) -> bool:
    """Удалить один ключ. Возвращает True если ключ существовал."""
    r = await get_redis()
    deleted = await r.delete(key)
    logger.info("CACHE INVALIDATE  %s  (existed=%s)", key, bool(deleted))
    return bool(deleted)


async def cache_delete_many(keys: list[str]) -> int:
    """Batch invalidate — удалить список ключей одной операцией."""
    if not keys:
        return 0
    r = await get_redis()
    deleted = await r.delete(*keys)
    for k in keys:
        logger.info("CACHE INVALIDATE  %s", k)
    logger.info("CACHE INVALIDATE BATCH  count=%d  deleted=%d", len(keys), deleted)
    return deleted


async def cache_delete_by_pattern(pattern: str) -> int:
    """
    Инвалидация по шаблону через SCAN (безопасно для production).
    НЕ используем KEYS — он блокирует Redis.
    """
    r = await get_redis()
    deleted_total = 0
    cursor = 0
    while True:
        cursor, keys = await r.scan(cursor=cursor, match=pattern, count=100)
        if keys:
            deleted = await r.delete(*keys)
            deleted_total += deleted
            for k in keys:
                logger.info("CACHE INVALIDATE (scan)  %s", k)
        if cursor == 0:
            break
    logger.info("CACHE INVALIDATE PATTERN  pattern=%s  deleted=%d", pattern, deleted_total)
    return deleted_total


async def cache_get_all_keys() -> list[str]:
    """Получить все ключи (для диагностики). Использует SCAN."""
    r = await get_redis()
    all_keys: list[str] = []
    cursor = 0
    while True:
        cursor, keys = await r.scan(cursor=cursor, count=200)
        all_keys.extend(keys)
        if cursor == 0:
            break
    return sorted(all_keys)
