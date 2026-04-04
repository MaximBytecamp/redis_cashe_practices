"""
FastAPI приложение — точка входа, lifecycle, middleware.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.cache.redis_client import close_redis, get_redis
from app.routers.products import router as products_router
from app.seed import seed_database

# ── Логирование 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)-14s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("app")


# ── Lifecycle
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Запуск приложения…")

    # Redis
    await get_redis()
    logger.info("Redis подключён")

    # БД + seed
    count = await seed_database()
    logger.info("БД готова, товаров: %d", count)

    yield

    await close_redis()
    logger.info("Redis отключён")


app = FastAPI(
    title="Invalidate After Update — Demo",
    description=(
        "Практика cache-aside + invalidate after update.\n\n"
        "• Чтение через Redis (cache-aside)\n"
        "• После update → invalidate связанных кэшей\n"
        "• Логи: HIT / MISS / SET / INVALIDATE"
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(products_router)


@app.get("/health")
async def health():
    r = await get_redis()
    return {"status": "ok", "redis": await r.ping()}
