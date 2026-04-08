from __future__ import annotations

import datetime
from typing import Any

from sqlalchemy import func, select

from app.db import async_session
from app.models.product import Product


class ProductRepository:
    """CRUD-операции над таблицей products."""

    # ── READ

    @staticmethod
    async def get_by_id(product_id: int) -> dict[str, Any] | None:
        async with async_session() as session:
            result = await session.execute(
                select(Product).where(Product.id == product_id)
            )
            p = result.scalar_one_or_none()
            return p.to_dict() if p else None

    @staticmethod
    async def get_all() -> list[dict[str, Any]]:
        async with async_session() as session:
            result = await session.execute(
                select(Product).order_by(Product.id)
            )
            return [p.to_dict() for p in result.scalars().all()]
        

    @staticmethod
    async def get_by_category(category: str) -> list[dict[str, Any]]:
        async with async_session() as session:
            result = await session.execute(
                select(Product)
                .where(Product.category == category)
                .order_by(Product.id)
            )
            return [p.to_dict() for p in result.scalars().all()]

    @staticmethod
    async def get_stats() -> dict[str, Any]:
        async with async_session() as session:
            total = (await session.execute(select(func.count(Product.id)))).scalar() or 0
            active = (await session.execute(
                select(func.count(Product.id)).where(Product.is_active == True)
            )).scalar() or 0
            avg_price = (await session.execute(select(func.avg(Product.price)))).scalar() or 0.0
            in_stock = (await session.execute(
                select(func.count(Product.id)).where(Product.stock > 0)
            )).scalar() or 0

            cats_q = await session.execute(
                select(Product.category, func.count(Product.id))
                .group_by(Product.category)
            )
            by_cat = {cat: cnt for cat, cnt in cats_q.all()}

            return {
                "total_products": total,
                "active_products": active,
                "average_price": round(float(avg_price), 2),
                "in_stock_products": in_stock,
                "products_by_category": by_cat,
            }
        
    @staticmethod
    async def get_categories() -> list[str]:
        async with async_session() as session:
            result = await session.execute(
                select(Product.category).distinct().order_by(Product.category)
            )
            return [row[0] for row in result.all()] #[(User,), (User,)] -> [User, User]
        
    
        # ── WRITE 

    @staticmethod
    async def update_product(
        product_id: int, data: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Обновляет товар, возвращает НОВЫЙ dict (для write-through)."""
        async with async_session() as session:
            result = await session.execute(
                select(Product).where(Product.id == product_id)
            )
            product = result.scalar_one_or_none()
            if product is None:
                return None

            for field, value in data.items():
                if hasattr(product, field):
                    setattr(product, field, value)
            product.updated_at = datetime.datetime.now()

            await session.commit()
            await session.refresh(product)
            return product.to_dict()

    @staticmethod
    async def batch_update_prices(
        category: str, multiplier: float
    ) -> list[dict[str, Any]]:
        """Массовое обновление цен в категории. Возвращает обновлённые товары."""
        async with async_session() as session:
            result = await session.execute(
                select(Product).where(Product.category == category)
            )
            products = result.scalars().all()
            now = datetime.datetime.now()
            for p in products:
                p.price = round(p.price * multiplier, 2)
                p.updated_at = now
            await session.commit()

            # re-fetch
            result2 = await session.execute(
                select(Product).where(Product.category == category).order_by(Product.id)
            )
            return [p.to_dict() for p in result2.scalars().all()]