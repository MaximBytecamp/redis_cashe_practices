"""Cache-сервис — обёртка над Redis для кэширования товаров."""

from __future__ import annotations

from app.cache.helpers import cache_delete, cache_get, cache_set
from app.cache.keys import product_key, product_meta_key


async def get_cached_product(product_id: int) -> dict | None:
    """Прочитать товар из Redis."""
    key = product_key(product_id)
    return await cache_get(key)


async def set_cached_product(product_id: int, data: dict, ttl: int | None = None) -> None:
    """Записать товар в Redis."""
    key = product_key(product_id)
    await cache_set(key, data, ttl=ttl)


async def invalidate_product(product_id: int) -> int:
    """Удалить товар и его метаданные из кэша."""
    key = product_key(product_id)
    meta_key = product_meta_key(product_id)
    deleted = await cache_delete(key)
    await cache_delete(meta_key)
    return deleted
