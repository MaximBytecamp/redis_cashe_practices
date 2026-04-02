
from __future__ import annotations

import datetime
import logging
from collections import Counter

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.product import Product

logger = logging.getLogger("repository")


class ProductRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    # ── Read

    async def get_by_id(self, product_id: int) -> Product | None:
        logger.info("DB SELECT  product id=%d", product_id)
        result = await self.session.execute(
            select(Product).where(Product.id == product_id)
        )
        return result.scalar_one_or_none()

    async def get_all(self) -> list[Product]:
        logger.info("DB SELECT  all products")
        result = await self.session.execute(
            select(Product).order_by(Product.id)
        )
        return list(result.scalars().all())

    async def get_by_category(self, category: str) -> list[Product]:
        logger.info("DB SELECT  products category=%s", category)
        result = await self.session.execute(
            select(Product).where(Product.category == category).order_by(Product.id)
        )
        return list(result.scalars().all())

    async def get_stats(self) -> dict:
        logger.info("DB SELECT  stats")
        products = await self.get_all()
        total = len(products)
        in_stock = sum(1 for p in products if p.stock > 0)
        avg_price = sum(p.price for p in products) / total if total else 0
        categories = dict(Counter(p.category for p in products))
        return {
            "total_products": total,
            "in_stock": in_stock,
            "out_of_stock": total - in_stock,
            "avg_price": round(avg_price, 2),
            "categories": categories,
        }

    # ── Write 

    async def update_product(self, product_id: int, data: dict) -> Product | None:
        """
        Обновить товар. Возвращает обновлённый Product или None.
        Commit делается СНАРУЖИ (в service), чтобы invalidate был после commit.
        """
        product = await self.get_by_id(product_id)
        if product is None:
            return None

        for field, value in data.items():
            if value is not None:
                setattr(product, field, value)
        product.updated_at = datetime.datetime.utcnow()

        logger.info("DB UPDATE  product id=%d  fields=%s", product_id, list(data.keys()))
        return product

    async def apply_discount(self, category: str, percent: float) -> list[Product]:
        """
        Применить скидку ко всем товарам категории.
        Возвращает список изменённых товаров.
        """
        products = await self.get_by_category(category)
        multiplier = 1 - percent / 100
        now = datetime.datetime.utcnow()

        for p in products:
            p.price = round(p.price * multiplier, 2)
            p.updated_at = now

        logger.info(
            "DB UPDATE BATCH  category=%s  discount=%.1f%%  affected=%d",
            category, percent, len(products),
        )
        return products
