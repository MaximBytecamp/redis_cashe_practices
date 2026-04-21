"""Repository — прямой доступ к БД."""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import select

from app.db import async_session
from models.product import Product

logger = logging.getLogger("repository")

# Счётчик обращений к БД
db_read_count: int = 0


class ProductRepository:
    """CRUD-операции над таблицей products."""

    @staticmethod
    async def get_by_id(product_id: int) -> dict[str, Any] | None:
        global db_read_count
        db_read_count += 1
        logger.info("[DB READ]  product id=%d  (total=%d)", product_id, db_read_count)
        async with async_session() as session:
            result = await session.execute(
                select(Product).where(Product.id == product_id)
            )
            p = result.scalar_one_or_none()
            return p.to_dict() if p else None

    @staticmethod
    async def get_all_ids() -> list[int]:
        """Получить все ID продуктов (для Bloom filter populate)."""
        async with async_session() as session:
            result = await session.execute(select(Product.id))
            return [row[0] for row in result.all()]

    @staticmethod
    async def create(data: dict[str, Any]) -> dict[str, Any]:
        logger.info("[DB CREATE]  product name=%s", data.get("name"))
        async with async_session() as session:
            product = Product(**data)
            session.add(product)
            await session.commit()
            await session.refresh(product)
            return product.to_dict()

    @staticmethod
    async def create_with_id(product_id: int, data: dict[str, Any]) -> dict[str, Any]:
        logger.info("[DB CREATE]  product id=%d", product_id)
        async with async_session() as session:
            product = Product(id=product_id, **data)
            session.add(product)
            await session.commit()
            await session.refresh(product)
            return product.to_dict()

    @staticmethod
    async def delete(product_id: int) -> bool:
        logger.info("[DB DELETE]  product id=%d", product_id)
        async with async_session() as session:
            result = await session.execute(
                select(Product).where(Product.id == product_id)
            )
            p = result.scalar_one_or_none()
            if p is None:
                return False
            await session.delete(p)
            await session.commit()
            return True
