from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import select

from app.db import async_session
from app.models.product import Product

logger = logging.getLogger("repository")


class ProductRepository:

    # ── READ

    @staticmethod
    async def get_by_id(product_id: int) -> dict[str, Any] | None:
        logger.info("[DB READ]  product id=%d", product_id)
        async with async_session() as session:
            result = await session.execute(
                select(Product).where(Product.id == product_id)
            )
            p = result.scalar_one_or_none()
            return p.to_dict() if p else None

    @staticmethod
    async def get_all() -> list[dict[str, Any]]:
        logger.info("[DB READ]  all products")
        async with async_session() as session:
            result = await session.execute(select(Product).order_by(Product.id))
            return [p.to_dict() for p in result.scalars().all()]
        
    # ── CREATE

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
        """Создать товар с заданным ID (для тестов null cache)."""
        logger.info("[DB CREATE]  product id=%d name=%s", product_id, data.get("name"))
        async with async_session() as session:
            product = Product(id=product_id, **data)
            session.add(product)
            await session.commit()
            await session.refresh(product)
            return product.to_dict()
        
    # ── UPDATE

    @staticmethod
    async def update(product_id: int, data: dict[str, Any]) -> dict[str, Any] | None:
        logger.info("[DB UPDATE]  product id=%d", product_id)
        async with async_session() as session:
            result = await session.execute(
                select(Product).where(Product.id == product_id)
            )
            product = result.scalar_one_or_none()
            if product is None:
                return None
            for field, value in data.items():
                if hasattr(product, field) and value is not None:
                    setattr(product, field, value)
            await session.commit()
            await session.refresh(product)
            return product.to_dict()
        
    # ── DELETE

    @staticmethod
    async def delete(product_id: int) -> bool:
        logger.info("[DB DELETE]  product id=%d", product_id)
        async with async_session() as session:
            result = await session.execute(
                select(Product).where(Product.id == product_id)
            )
            product = result.scalar_one_or_none()
            if product is None:
                return False
            await session.delete(product)
            await session.commit()
            return True

    @staticmethod
    async def max_id() -> int:
        """Максимальный ID в таблице."""
        async with async_session() as session:
            from sqlalchemy import func
            result = await session.execute(select(func.max(Product.id)))
            return result.scalar() or 0