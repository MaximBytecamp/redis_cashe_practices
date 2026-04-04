"""
Product Service — бизнес-логика с cache-aside + invalidate after update.
"""

from __future__ import annotations

import logging

from sqlalchemy.ext.asyncio import AsyncSession

from app.cache.helpers import cache_get, cache_set
from app.cache.keys import (
    product_key,
    products_all_key,
    products_by_category_key,
    products_stats_key,
)
from app.config import settings
from app.repositories.product_repository import ProductRepository
from app.services.cache_invalidation_service import invalidation_service

logger = logging.getLogger("service")


class ProductService:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.repo = ProductRepository(session)

    # READ — Cache-Aside

    async def get_product(self, product_id: int) -> dict | None:
        """
        Чтение товара: Redis → DB.
        1. Проверить кэш
        2. Если hit → вернуть
        3. Если miss → читать из БД → записать в кэш → вернуть
        """
        key = product_key(product_id)

        # 1. Cache lookup
        cached = await cache_get(key)
        if cached is not None:
            return cached

        # 2. DB fallback
        product = await self.repo.get_by_id(product_id)
        if product is None:
            return None

        data = product.to_dict()

        # 3. Cache fill
        await cache_set(key, data)
        return data

    async def get_all_products(self) -> list[dict]:
        """Все товары с кэшированием."""
        key = products_all_key()

        cached = await cache_get(key)
        if cached is not None:
            return cached

        products = await self.repo.get_all()
        data = [p.to_dict() for p in products]

        await cache_set(key, data)
        return data

    async def get_products_by_category(self, category: str) -> list[dict]:
        """Товары по категории с кэшированием."""
        key = products_by_category_key(category)

        cached = await cache_get(key)
        if cached is not None:
            return cached

        products = await self.repo.get_by_category(category)
        data = [p.to_dict() for p in products]

        await cache_set(key, data)
        return data

    async def get_stats(self) -> dict:
        """Статистика с кэшированием."""
        key = products_stats_key()

        cached = await cache_get(key)
        if cached is not None:
            return cached

        stats = await self.repo.get_stats()
        await cache_set(key, stats)
        return stats


    # WRITE — Update + Invalidate After Update

    async def update_product(self, product_id: int, data: dict) -> dict | None:
        """
        Обновление товара по паттерну Invalidate After Update:

        1. Запомнить старую категорию
        2. Обновить в БД
        3. COMMIT — данные зафиксированы
        4. Инвалидировать связанные кэши
           (только ПОСЛЕ успешного commit!)
        """
        # 1. Получить текущий товар (для old_category)
        old_product = await self.repo.get_by_id(product_id)
        if old_product is None:
            return None
        old_category = old_product.category

        # 2. Обновить в БД
        updated = await self.repo.update_product(product_id, data)
        if updated is None:
            return None

        # 3. COMMIT — источник истины обновлён
        await self.session.commit()
        logger.info("DB COMMIT  product id=%d", product_id)

        # 4. Invalidate ПОСЛЕ commit
        if settings.invalidate_enabled:
            new_category = updated.category
            await invalidation_service.invalidate_product(
                product_id=product_id,
                old_category=old_category,
                new_category=new_category,
            )
        else:
            logger.warning("INVALIDATE DISABLED — stale cache possible!")

        # Обновляем сессию чтобы получить свежие данные
        await self.session.refresh(updated)
        return updated.to_dict()

    async def apply_discount(self, category: str, percent: float) -> list[dict]:
        """
        Массовое обновление + групповая инвалидация.

        1. Применить скидку ко всем товарам категории
        2. COMMIT
        3. Инвалидировать все затронутые ключи
        """
        # 1. Обновить в БД
        products = await self.repo.apply_discount(category, percent)
        product_ids = [p.id for p in products]

        # 2. COMMIT
        await self.session.commit()
        logger.info("DB COMMIT BATCH  category=%s  count=%d", category, len(products))

        # 3. Invalidate
        if settings.invalidate_enabled:
            await invalidation_service.invalidate_category_batch(
                category=category,
                product_ids=product_ids,
            )

        return [p.to_dict() for p in products]
