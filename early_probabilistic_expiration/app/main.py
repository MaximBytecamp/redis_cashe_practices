"""FastAPI application — Early Probabilistic Expiration (XFetch)."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.cache.redis_client import close_redis, flush_redis
from app.db import engine
from app.models.product import Base
from app.routes.products import router
from app.seed import seed_database

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)-18s  %(levelname)-5s  %(message)s",
    datefmt="%H:%M:%S",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    count = await seed_database()
    logging.getLogger("app").info("Seeded %d products", count)
    await flush_redis()
    yield
    # shutdown
    await close_redis()


app = FastAPI(
    title="Early Probabilistic Expiration (XFetch)",
    description=(
        "Вероятностный пересчёт кэша ДО истечения TTL. "
        "Предотвращает cache stampede без mutex lock."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(router)


@app.get("/health")
async def health():
    return {"status": "ok", "project": "early_probabilistic_expiration"}
