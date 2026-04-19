from __future__ import annotations

import logging
from typing import Any

from app.cache import helpers as ch
from app.cache import keys as ck
from app.cache.constaints import NULL_CACHE_MARKER
from app.config import settings

logger = logging.getLogger("cache_service")


class CacheService:
    """Null-aware кеш-логика для продуктов."""

    @staticmethod
    async def get_product(product_id: int) -> ch.CacheResult:
        key = ck.product_key(product_id)
        return await ch.cache_get(key)

    @staticmethod
    async def set_product(product_id: int, data: dict[str, Any]) -> None:
        key = ck.product_key(product_id)
        await ch.cache_set(key, data, settings.product_ttl)

    @staticmethod
    async def set_null(product_id: int) -> None:
        """Записать null marker (короткий TTL + jitter).

        Вызывается только если null_caching_enabled == True.
        """
        if not settings.null_caching_enabled:
            logger.info("[NULL CACHE SKIP] product:%d — null caching disabled", product_id)
            return
        key = ck.product_key(product_id)
        await ch.cache_set_null(key, settings.null_ttl)

    @staticmethod
    async def invalidate(product_id: int) -> int:
        key = ck.product_key(product_id)
        return await ch.cache_delete(key)
    
    @staticmethod
    async def get_debug(product_id: int) -> dict[str, Any]:
        """Отладочная информация: raw value + TTL."""
        key = ck.product_key(product_id)
        raw = await ch.cache_get_raw(key)
        ttl = await ch.cache_get_ttl(key)
        is_null = raw == NULL_CACHE_MARKER if raw is not None else None
        return {
            "key": key,
            "raw_value": raw,
            "is_null_marker": is_null,
            "ttl_seconds": ttl,
            "state": "null_hit" if is_null else ("hit" if raw else "miss"),
        }
    



