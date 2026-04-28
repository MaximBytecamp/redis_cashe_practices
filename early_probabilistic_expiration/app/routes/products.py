"""Маршруты — товары + debug endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.cache.redis_client import flush_redis
from app.config import settings
from app.repositories import product_repository
from app.services.product_service import get_product

router = APIRouter()


@router.get("/products/{product_id}")
async def read_product(
    product_id: int,
    xfetch: bool | None = Query(
        default=None,
        description="true=XFetch, false=обычный cache-aside, omit=use global setting",
    ),
):
    result = await get_product(product_id, xfetch=xfetch)

    if result.data is None:
        raise HTTPException(status_code=404, detail="Product not found")

    return {
        "product": result.data,
        "_meta": {
            "source": result.source,
            "delta": round(result.delta, 6),
            "ttl_remaining": round(result.ttl_remaining, 2),
            "recomputed": result.recomputed,
            "probability_gap": round(result.probability, 6),
        },
    }


# ─── Debug endpoints ─────────────────────────────────────────────────

@router.post("/debug/flush-cache")
async def debug_flush_cache():
    await flush_redis()
    return {"status": "ok", "action": "cache flushed"}


@router.post("/debug/reset-counters")
async def debug_reset_counters():
    product_repository.reset_db_read_count()
    return {"status": "ok", "db_read_count": 0}


@router.get("/debug/counters")
async def debug_counters():
    return {"db_read_count": product_repository.get_db_read_count()}


class DelayRequest(BaseModel):
    delay: float = 0.0


@router.post("/debug/set-db-delay")
async def debug_set_db_delay(req: DelayRequest):
    product_repository.set_simulate_delay(req.delay)
    return {"status": "ok", "db_simulate_delay": req.delay}


class BetaRequest(BaseModel):
    beta: float = 1.0


@router.post("/debug/set-beta")
async def debug_set_beta(req: BetaRequest):
    settings.xfetch_beta = req.beta
    return {"status": "ok", "xfetch_beta": req.beta}


class TTLRequest(BaseModel):
    ttl: int = 120


@router.post("/debug/set-ttl")
async def debug_set_ttl(req: TTLRequest):
    settings.product_cache_ttl = req.ttl
    return {"status": "ok", "product_cache_ttl": req.ttl}


class ToggleXFetchRequest(BaseModel):
    enabled: bool


@router.post("/debug/toggle-xfetch")
async def debug_toggle_xfetch(req: ToggleXFetchRequest):
    settings.xfetch_enabled = req.enabled
    return {"status": "ok", "xfetch_enabled": req.enabled}


@router.get("/debug/config")
async def debug_config():
    return {
        "product_cache_ttl": settings.product_cache_ttl,
        "xfetch_beta": settings.xfetch_beta,
        "xfetch_enabled": settings.xfetch_enabled,
        "db_simulate_delay": product_repository.get_simulate_delay(),
    }
