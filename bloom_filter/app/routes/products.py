"""HTTP-маршруты — CRUD + debug endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.cache.redis_client import flush_redis
from app.config import settings
from app.repositories.product_repository import ProductRepository
from app.services.bloom_service import (
    bloom_memory_bytes,
    bloom_populate,
    get_bloom_stats,
    reset_bloom_stats,
)
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
                "_layers": result.get("_layers", []),
            },
        )
    return result


# ── CREATE / DELETE 

@router.post("/products", status_code=201)
async def create_product(body: dict, product_id: int | None = Query(default=None)):
    result = await ProductService.create_product(body, product_id=product_id)
    return result


@router.delete("/products/{product_id}")
async def delete_product(product_id: int):
    result = await ProductService.delete_product(product_id)
    if result["_status"] == 404:
        raise HTTPException(404, detail="Product not found")
    return result


# ── DEBUG / ADMIN 

@router.get("/debug/product/{product_id}")
async def debug_product(product_id: int):
    return await ProductService.get_cache_debug(product_id)


@router.post("/debug/flush-cache")
async def debug_flush():
    """Очистить кеш + negative cache (без Bloom filter)."""
    from app.cache.redis_client import get_redis
    r = await get_redis()
    # Удалить все product:* и neg:* ключи, но оставить bloom:*
    cursor = 0
    while True:
        cursor, keys = await r.scan(cursor, match="product:*", count=100)
        if keys:
            await r.delete(*keys)
        if cursor == 0:
            break
    cursor = 0
    while True:
        cursor, keys = await r.scan(cursor, match="neg:*", count=100)
        if keys:
            await r.delete(*keys)
        if cursor == 0:
            break
    return {"status": "flushed (cache + negative, bloom preserved)"}


@router.post("/debug/flush-all")
async def debug_flush_all():
    """Очистить ВСЁ включая Bloom filter."""
    await flush_redis()
    return {"status": "flushed all (including bloom)"}


@router.post("/debug/rebuild-bloom")
async def debug_rebuild_bloom():
    """Пересобрать Bloom filter из текущих данных БД."""
    ids = await ProductRepository.get_all_ids()
    count = await bloom_populate(ids)
    mem = await bloom_memory_bytes()
    return {"status": "rebuilt", "items": count, "memory_bytes": mem}


@router.get("/debug/counters")
async def debug_counters():
    return get_counters()


@router.post("/debug/reset-counters")
async def debug_reset_counters():
    reset_counters()
    reset_bloom_stats()
    return {"status": "reset"}


@router.get("/debug/bloom-stats")
async def debug_bloom_stats():
    stats = get_bloom_stats()
    mem = await bloom_memory_bytes()
    return {**stats, "memory_bytes": mem}


@router.post("/debug/set-bloom")
async def debug_set_bloom(enabled: bool = Query(...)):
    settings.bloom_enabled = enabled
    return {"bloom_enabled": settings.bloom_enabled}


@router.post("/debug/set-negative-cache")
async def debug_set_negative(enabled: bool = Query(...)):
    settings.negative_cache_enabled = enabled
    return {"negative_cache_enabled": settings.negative_cache_enabled}


@router.get("/debug/config")
async def debug_config():
    return {
        "bloom_enabled": settings.bloom_enabled,
        "negative_cache_enabled": settings.negative_cache_enabled,
        "product_ttl": settings.product_ttl,
        "negative_ttl": settings.negative_ttl,
        "bloom_expected_items": settings.bloom_expected_items,
        "bloom_fp_rate": settings.bloom_fp_rate,
    }
