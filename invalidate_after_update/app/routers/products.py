"""
Routes — все HTTP endpoints для продуктов.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.cache.helpers import cache_get_all_keys
from app.cache.redis_client import flush_redis
from app.config import settings
from app.db import get_session
from app.schemas.product import DiscountRequest, ProductRead, ProductStats, ProductUpdate
from app.services.product_service import ProductService

router = APIRouter(prefix="/products", tags=["Products"])


def _get_service(session: AsyncSession = Depends(get_session)) -> ProductService:
    return ProductService(session)


# READ endpoints

@router.get("/stats", response_model=ProductStats)
async def get_stats(service: ProductService = Depends(_get_service)):
    """Статистика по товарам (кэшируется отдельно)."""
    return await service.get_stats()


@router.get("/category/{category}", response_model=list[ProductRead])
async def get_by_category(
    category: str,
    service: ProductService = Depends(_get_service),
):
    """Товары по категории."""
    return await service.get_products_by_category(category)


@router.get("/{product_id}", response_model=ProductRead)
async def get_product(
    product_id: int,
    service: ProductService = Depends(_get_service),
):
    """Карточка одного товара."""
    data = await service.get_product(product_id)
    if data is None:
        raise HTTPException(404, detail="Product not found")
    return data


@router.get("", response_model=list[ProductRead])
async def get_all_products(service: ProductService = Depends(_get_service)):
    """Все товары."""
    return await service.get_all_products()


# WRITE endpoints

@router.put("/{product_id}", response_model=ProductRead)
async def update_product(
    product_id: int,
    body: ProductUpdate,
    service: ProductService = Depends(_get_service),
):
    """
    Обновить товар.
    После успешного update — invalidate связанных кэшей.
    """
    update_data = body.model_dump(exclude_unset=True)
    if not update_data:
        raise HTTPException(400, detail="No fields to update")

    result = await service.update_product(product_id, update_data)
    if result is None:
        raise HTTPException(404, detail="Product not found")
    return result


@router.patch("/category/{category}/discount", response_model=list[ProductRead])
async def apply_discount(
    category: str,
    body: DiscountRequest,
    service: ProductService = Depends(_get_service),
):
    """
    Применить скидку ко всем товарам категории.
    Массовый update + групповая инвалидация.
    """
    products = await service.apply_discount(category, body.percent)
    if not products:
        raise HTTPException(404, detail=f"No products in category '{category}'")
    return products



# Admin / Debug endpoints

@router.get("/debug/cache-keys", tags=["Debug"])
async def list_cache_keys():
    """Показать все ключи в Redis (диагностика)."""
    keys = await cache_get_all_keys()
    return {"count": len(keys), "keys": keys}


@router.post("/debug/flush-cache", tags=["Debug"])
async def flush_cache():
    """Сбросить весь кэш Redis."""
    await flush_redis()
    return {"status": "flushed"}


@router.post("/debug/toggle-invalidate", tags=["Debug"])
async def toggle_invalidate(enabled: bool = Query(...)):
    """Включить/выключить invalidate (для демонстрации stale cache)."""
    settings.invalidate_enabled = enabled
    return {"invalidate_enabled": settings.invalidate_enabled}
