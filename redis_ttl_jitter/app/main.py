from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

from app.cashe import (
    configure_cache,
    current_config,
    get_product,
    local_cache,
)
from app.database import db_stats
from app.metrics import Timer, metrics
from app.redis_client import close_redis, flush_redis, get_redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)-10s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("app")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Подключение к Redis…")
    await get_redis()
    logger.info("Redis подключён")
    yield
    await close_redis()
    logger.info("Redis отключён")


app = FastAPI(
    title="Redis TTL + Jitter Demo",
    description="Демонстрация TTL, random jitter, anti-stampede, null-caching",
    version="1.0.0",
    lifespan=lifespan,
)

@app.get("/product/{product_id}")
async def read_product(product_id: int):
    with Timer():
        data = await get_product(product_id)

    if data is None:
        return JSONResponse({"detail": "Product not found"}, status_code=404)
    return data


@app.get("/metrics")
async def read_metrics():
    return {
        "cache": metrics.summary(),
        "db": {
            "total_queries": db_stats.total_queries,
            "avg_db_latency_ms": (
                round(sum(db_stats.latencies) / len(db_stats.latencies) * 1000, 2)
                if db_stats.latencies
                else 0
            ),
        },
        "config": current_config(),
    }


@app.post("/metrics/reset")
async def reset_metrics():
    metrics.reset()
    db_stats.reset()
    return {"status": "reset"}


@app.post("/cache/flush")
async def flush_cache():
    await flush_redis()
    local_cache.clear()
    return {"status": "flushed"}


@app.post("/cache/configure")
async def configure(
    use_jitter: bool | None = Query(None),
    use_mutex: bool | None = Query(None),
    use_null_cache: bool | None = Query(None),
    use_local_cache: bool | None = Query(None),
    use_early_expiration: bool | None = Query(None),
):
    """
    Переключить режимы кэширования на лету.

    Примеры:
      POST /cache/configure?use_jitter=false        — отключить jitter
      POST /cache/configure?use_mutex=false          — отключить anti-stampede
    """
    cfg = configure_cache(
        use_jitter=use_jitter,
        use_mutex=use_mutex,
        use_null_cache=use_null_cache,
        use_local_cache=use_local_cache,
        use_early_expiration=use_early_expiration,
    )
    return {"config": cfg}


@app.get("/cache/config")
async def get_config():
    return {"config": current_config()}


@app.get("/health")
async def health():
    """Health check."""
    r = await get_redis()
    redis_ok = await r.ping()
    return {"status": "ok", "redis": redis_ok}
