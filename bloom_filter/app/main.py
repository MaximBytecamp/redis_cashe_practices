"""FastAPI application — Bloom Filter + Negative Cache."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.cache.redis_client import close_redis, flush_redis
from app.db import engine
from models.product import Base
from app.repositories.product_repository import ProductRepository
from app.routes.products import router
from app.seed import seed_database
from app.services.bloom_service import bloom_populate

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

    # Populate Bloom filter with all existing product IDs
    all_ids = await ProductRepository.get_all_ids()
    await bloom_populate(all_ids)
    logging.getLogger("app").info("Bloom filter populated with %d IDs", len(all_ids))

    yield
    # shutdown
    await close_redis()


app = FastAPI(
    title="Bloom Filter + Negative Cache",
    description="Трёхуровневая защита от cache penetration",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(router)


@app.get("/health")
async def health():
    return {"status": "ok", "project": "bloom_filter_negative_cache"}
