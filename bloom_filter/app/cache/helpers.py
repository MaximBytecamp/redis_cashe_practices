from __future__ import annotations

import json
import logging
import random
from dataclasses import dataclass
from typing import Any

from app.cache.constants import NEGATIVE_MARKER
from app.cache.keys import negative_key, product_key
from app.cache.redis_client import get_redis
from app.config import settings

logger = logging.getLogger("cache")


@dataclass
class CacheResult:
    """Результат чтения из кеша."""
    state: str   # "hit" | "neg_hit" | "miss"
    data: Any    # dict для hit, None для neg_hit/miss


async def cache_get(product_id: int) -> CacheResult:
    r = await get_redis()
    key = product_key(product_id)
    raw = await r.get(key)

    if raw is None:
        logger.info("[CACHE MISS]      %s", key)
        return CacheResult(state="miss", data=None)

    logger.info("[CACHE HIT]       %s", key)
    return CacheResult(state="hit", data=json.loads(raw))


async def cache_set(product_id: int, value: Any, ttl: int | None = None) -> None:
    r = await get_redis()
    key = product_key(product_id)
    ttl = ttl or settings.product_ttl
    await r.set(key, json.dumps(value, default=str), ex=ttl)
    logger.info("[CACHE SET]       %s  ttl=%ds", key, ttl)


async def cache_delete(product_id: int) -> int:
    r = await get_redis()
    key = product_key(product_id)
    deleted = await r.delete(key)
    if deleted:
        logger.info("[CACHE DELETE]    %s", key)
    return deleted


async def negative_get(product_id: int) -> bool:
    r = await get_redis()
    key = negative_key(product_id)
    raw = await r.get(key)
    if raw == NEGATIVE_MARKER:
        logger.info("[NEG CACHE HIT]   %s", key)
        return True
    return False


async def negative_set(product_id: int, ttl: int | None = None) -> None:
    r = await get_redis()
    key = negative_key(product_id)
    base_ttl = ttl or settings.negative_ttl
    jitter = random.randint(0, max(1, base_ttl // 5))
    final_ttl = base_ttl + jitter
    await r.set(key, NEGATIVE_MARKER, ex=final_ttl)
    logger.info("[NEG CACHE SET]   %s  ttl=%ds (base=%d jitter=%d)", key, final_ttl, base_ttl, jitter)


async def negative_delete(product_id: int) -> None:
    r = await get_redis()
    key = negative_key(product_id)
    deleted = await r.delete(key)
    if deleted:
        logger.info("[NEG CACHE DEL]   %s", key)


async def cache_get_ttl(product_id: int) -> int:
    r = await get_redis()
    return await r.ttl(product_key(product_id))


async def negative_get_ttl(product_id: int) -> int:
    r = await get_redis()
    return await r.ttl(negative_key(product_id))
