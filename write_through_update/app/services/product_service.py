from __future__ import annotations

import logging
import time
from typing import Any

from app.cache import helpers as ch
from app.cache import keys as ck
from app.repositories.product_repository import ProductRepository
from app.services.cache_sync_service import CacheSyncService

logger = logging.getLogger("product_service")


class ProductService:
    """Высокоуровневый сервис с кешем."""

    # ── READ (Cache-Aside)
    @staticmethod
    async def get_product(product_id: int) -> dict[str, Any] | None:
        key = ck.product_key(product_id)
        cached = await ch.cache_get(key)
        if cached is not None:
            return {**cached, "_source": "cache"}

        data = await ProductRepository.get_by_id(product_id)
        if data is None:
            return None
        await ch.cache_set(key, data)
        return {**data, "_source": "db"}
    

    @staticmethod
    async def get_all_products() -> dict[str, Any]:
        key = ck.products_all_key()
        cached = await ch.cache_get(key)
        if cached is not None:
            return {"products": cached, "_source": "cache", "_count": len(cached)}

        products = await ProductRepository.get_all()
        await ch.cache_set(key, products)
        return {"products": products, "_source": "db", "_count": len(products)}

    @staticmethod
    async def get_by_category(category: str) -> dict[str, Any]:
        key = ck.products_category_key(category)
        cached = await ch.cache_get(key)
        if cached is not None:
            return {"products": cached, "_source": "cache", "_count": len(cached)}

        products = await ProductRepository.get_by_category(category)
        await ch.cache_set(key, products)
        return {"products": products, "_source": "db", "_count": len(products)}

    @staticmethod
    async def get_stats() -> dict[str, Any]:
        key = ck.products_stats_key()
        cached = await ch.cache_get(key)
        if cached is not None:
            return {**cached, "_source": "cache"}

        stats = await ProductRepository.get_stats()
        await ch.cache_set(key, stats)
        return {**stats, "_source": "db"}
    

   # ── WRITE + SYNC

    @staticmethod
    async def update_product(
        product_id: int, data: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Обновляет товар в БД, затем синхронизирует кеш."""

        # Получаем старые данные (нужна old_category для invalidate)
        old = await ProductRepository.get_by_id(product_id)
        if old is None:
            return None
        old_category = old["category"]

        # Пишем в БД
        t0 = time.perf_counter()
        updated = await ProductRepository.update_product(product_id, data)
        db_ms = (time.perf_counter() - t0) * 1000

        if updated is None:
            return None

        new_category = updated["category"]

        # Синхронизируем кеш
        t1 = time.perf_counter()
        sync_metrics = await CacheSyncService.after_product_update(
            product_id=product_id,
            new_data=updated,
            old_category=old_category,
            new_category=new_category,
        )
        sync_ms = (time.perf_counter() - t1) * 1000

        return {
            **updated,
            "_db_write_ms": round(db_ms, 2),
            "_sync_ms": round(sync_ms, 2),
            "_sync": sync_metrics,
        }
    

    @staticmethod
    async def batch_update_prices(
        category: str, multiplier: float
    ) -> dict[str, Any]:
        """Массовое обновление цен в категории."""
        t0 = time.perf_counter()
        updated = await ProductRepository.batch_update_prices(category, multiplier)
        db_ms = (time.perf_counter() - t0) * 1000

        t1 = time.perf_counter()
        sync_metrics = await CacheSyncService.after_batch_update(category, updated)
        sync_ms = (time.perf_counter() - t1) * 1000

        return {
            "updated_count": len(updated),
            "category": category,
            "multiplier": multiplier,
            "products": updated,
            "_db_write_ms": round(db_ms, 2),
            "_sync_ms": round(sync_ms, 2),
            "_sync": sync_metrics,
        }
    
    # ── DEBUG / ADMIN

    @staticmethod
    async def get_cache_debug(product_id: int) -> dict[str, Any]:
        """Возвращает кешированное и БД-значение для сравнения."""
        key = ck.product_key(product_id)
        cached = await ch.cache_get(key)
        db_val = await ProductRepository.get_by_id(product_id)
        is_consistent = cached == db_val if cached and db_val else None
        return {
            "product_id": product_id,
            "cached": cached,
            "database": db_val,
            "is_consistent": is_consistent,
        }