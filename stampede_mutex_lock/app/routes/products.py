"""Маршруты — товары + debug endpoints.

Endpoints:
  GET  /products/{id}          — получить товар (query: protection=true/false)
  POST /debug/flush-cache      — очистить Redis DB
  POST /debug/reset-counters   — обнулить счётчик DB reads
  GET  /debug/counters         — текущий счётчик DB reads
  POST /debug/set-db-delay     — установить имитацию задержки БД
  POST /debug/toggle-protection — вкл/выкл stampede-защиту
  GET  /debug/config           — текущие параметры
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.cache.redis_client import flush_redis
from app.config import settings
from app.repositories import product_repository
from app.services.product_service import get_product

router = APIRouter()


# ─── Основной endpoint ───────────────────────────────────────────────

@router.get("/products/{product_id}")
async def read_product(
    product_id: int,
    protection: bool | None = Query(
        default=None,
        description="true=mutex lock, false=no protection, omit=use global setting",
    ),
):
    """Получить товар по ID.

    Выбор стратегии через query-param `protection` или глобальную настройку.
    """
    result = await get_product(product_id, protection=protection)

    if result.data is None and result.source == "fallback":
        raise HTTPException(
            status_code=503,
            detail={
                "error": "Service temporarily unavailable",
                "reason": "lock_timeout",
                "retries_used": result.retries_used,
            },
        )

    if result.data is None:
        raise HTTPException(status_code=404, detail="Product not found")

    return {
        "product": result.data,
        "_meta": {
            "source": result.source,
            "retries_used": result.retries_used,
            "lock_owner": result.lock_owner,
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


class ToggleProtectionRequest(BaseModel):
    enabled: bool


@router.post("/debug/toggle-protection")
async def debug_toggle_protection(req: ToggleProtectionRequest):
    settings.stampede_protection_enabled = req.enabled
    return {"status": "ok", "stampede_protection_enabled": req.enabled}


@router.get("/debug/config")
async def debug_config():
    return {
        "product_cache_ttl": settings.product_cache_ttl,
        "lock_ttl_seconds": settings.lock_ttl_seconds,
        "lock_retry_delay_ms": settings.lock_retry_delay_ms,
        "lock_max_retries": settings.lock_max_retries,
        "db_simulate_delay": product_repository.get_simulate_delay(),
        "stampede_protection_enabled": settings.stampede_protection_enabled,
    }
