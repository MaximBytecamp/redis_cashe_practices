from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.cache.redis_client import flush_redis
from app.config import settings
from app.services.product_service import ProductService, get_counters, reset_counters

router = APIRouter(prefix="/api", tags=["products"])


# ── READ 

@router.get("/products/{product_id}")
async def get_product(product_id: int):
    result = await ProductService.get_product(product_id)
    if result["_status"] == 404:
        raise HTTPException(
            status_code=404,
            detail={
                "message": "Product not found",
                "product_id": product_id,
                "_source": result["_source"],
            },
        )
    return result


# ── CREATE 

@router.post("/products", status_code=201)
async def create_product(body: dict, product_id: int | None = Query(default=None)):
    result = await ProductService.create_product(body, product_id=product_id)
    return result


@router.put("/products/{product_id}")
async def update_product(product_id: int, body: dict):
    result = await ProductService.update_product(product_id, body)
    if result["_status"] == 404:
        raise HTTPException(404, detail="Product not found")
    return result


@router.delete("/products/{product_id}")
async def delete_product(
    product_id: int,
    strategy: str = Query(default="write_null", regex="^(delete_only|write_null)$"),
):
    result = await ProductService.delete_product(product_id, strategy=strategy)
    if result["_status"] == 404:
        raise HTTPException(404, detail="Product not found")
    return result


# ── DEBUG / ADMIN

@router.get("/debug/product/{product_id}")
async def debug_product(product_id: int):
    return await ProductService.get_cache_debug(product_id)


@router.post("/debug/flush-cache")
async def debug_flush():
    await flush_redis()
    return {"status": "flushed"}


@router.get("/debug/counters")
async def debug_counters():
    return get_counters()


@router.post("/debug/reset-counters")
async def debug_reset_counters():
    reset_counters()
    return {"status": "reset"}


@router.get("/debug/null-caching")
async def debug_get_null_caching():
    return {"null_caching_enabled": settings.null_caching_enabled}


@router.post("/debug/null-caching")
async def debug_set_null_caching(enabled: bool = Query(...)):
    """Включить/выключить null caching на лету."""
    settings.null_caching_enabled = enabled
    return {"null_caching_enabled": settings.null_caching_enabled}