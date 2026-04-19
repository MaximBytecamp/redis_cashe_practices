from __future__ import annotations

import json
import logging
import random
from dataclasses import dataclass
from typing import Any

from app.cache.constaints import NULL_CACHE_MARKER
from app.cache.redis_client import get_redis
from app.config import settings

logger = logging.getLogger("cache")


@dataclass
class CacheResult:
    state: str   # "hit" | "null_hit" | "miss"
    data: Any    # dict для hit, None для null_hit/miss



async def cache_get(key: str) -> CacheResult:
    """Прочитать ключ с распознаванием null marker."""
    r = await get_redis()
    raw = await r.get(key)

    if raw is None:
        logger.info("[CACHE MISS]      %s", key)
        return CacheResult(state="miss", data=None)

    if raw == NULL_CACHE_MARKER:
        logger.info("[NULL CACHE HIT]  %s", key)
        return CacheResult(state="null_hit", data=None)

    logger.info("[CACHE HIT]       %s", key)
    return CacheResult(state="hit", data=json.loads(raw))



async def cache_set(key: str, value: Any, ttl: int | None = None) -> None:
    r = await get_redis()
    ttl = ttl or settings.product_ttl
    await r.set(key, json.dumps(value, default=str), ex=ttl)
    logger.info("[CACHE SET]       %s  ttl=%ds", key, ttl)


async def cache_set_null(key: str, ttl: int | None = None) -> None:
    r = await get_redis()
    base_ttl = ttl or settings.null_ttl
    jitter = random.randint(0, max(1, base_ttl // 5))
    final_ttl = base_ttl + jitter
    await r.set(key, NULL_CACHE_MARKER, ex=final_ttl)
    logger.info("[NULL CACHE SET]  %s  ttl=%ds (base=%d jitter=%d)", key, final_ttl, base_ttl, jitter)


async def cache_delete(key: str) -> int:
    r = await get_redis()
    deleted = await r.delete(key)
    if deleted:
        logger.info("[CACHE DELETE]    %s", key)
    return deleted


async def cache_get_ttl(key: str) -> int:
    r = await get_redis()
    return await r.ttl(key)


async def cache_get_raw(key: str) -> str | None:
    r = await get_redis()
    return await r.get(key)