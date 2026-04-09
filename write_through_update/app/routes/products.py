"""HTTP-маршруты — продукты + debug-эндпоинты."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.cache.redis_client import flush_redis
from app.config import settings
from app.services.product_service import ProductService

router = APIRouter(prefix="/api", tags=["products"])


# ── READ

@router.get("/products")
async def list_products():
    return await ProductService.get_all_products()


@router.get("/products/stats")
async def product_stats():
    return await ProductService.get_stats()


@router.get("/products/category/{category}")
async def products_by_category(category: str):
    return await ProductService.get_by_category(category)


@router.get("/products/{product_id}")
async def get_product(product_id: int):
    result = await ProductService.get_product(product_id)
    if result is None:
        raise HTTPException(404, "Product not found")
    return result


# ── WRITE 

@router.put("/products/{product_id}")
async def update_product(product_id: int, body: dict):
    result = await ProductService.update_product(product_id, body)
    if result is None:
        raise HTTPException(404, "Product not found")
    return result


@router.post("/products/batch-price")
async def batch_update_prices(
    category: str = Query(...),
    multiplier: float = Query(...),
):
    return await ProductService.batch_update_prices(category, multiplier)


# ── DEBUG / ADMIN 

@router.get("/debug/product/{product_id}")
async def debug_product(product_id: int):
    """Сравнение кеша и БД для товара."""
    return await ProductService.get_cache_debug(product_id)


@router.post("/debug/flush-cache")
async def debug_flush():
    """Полная очистка Redis (для тестов)."""
    await flush_redis()
    return {"status": "flushed"}


@router.get("/debug/sync-mode")
async def get_sync_mode():
    return {"sync_mode": settings.sync_mode}


@router.post("/debug/sync-mode")
async def set_sync_mode(mode: str = Query(...)):
    """Переключение режима синхронизации на лету."""
    allowed = {"write_through", "invalidate", "hybrid", "none"}
    if mode not in allowed:
        raise HTTPException(400, f"Mode must be one of {allowed}")
    settings.sync_mode = mode
    return {"sync_mode": settings.sync_mode, "message": f"Switched to {mode}"}
