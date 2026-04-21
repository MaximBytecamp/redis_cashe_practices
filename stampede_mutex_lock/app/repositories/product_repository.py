from __future__ import annotations

import asyncio
import logging
import threading

from sqlalchemy import select

from app.config import settings
from app.db import async_session
from app.models.product import Product

logger = logging.getLogger("repository")

# ── Атомарный счётчик DB reads (thread-safe)
_lock = threading.Lock()
_db_read_count: int = 0

# ── Переопределяемая задержка (для тестовых сценариев)
_simulate_delay: float = settings.db_simulate_delay


def get_db_read_count() -> int:
    with _lock:
        return _db_read_count


def reset_db_read_count() -> None:
    global _db_read_count
    with _lock:
        _db_read_count = 0


def set_simulate_delay(delay: float) -> None:
    global _simulate_delay
    _simulate_delay = delay


def get_simulate_delay() -> float:
    return _simulate_delay


def _increment_counter() -> int:
    global _db_read_count
    with _lock:
        _db_read_count += 1
        return _db_read_count


async def get_product_by_id(product_id: int) -> dict | None:
    # Имитация медленного запроса
    delay = _simulate_delay
    if delay > 0:
        logger.info("[DB DELAY]         product id=%d  sleeping %.2fs", product_id, delay)
        await asyncio.sleep(delay)

    async with async_session() as session:
        result = await session.execute(
            select(Product).where(Product.id == product_id)
        )
        product = result.scalar_one_or_none()

    count = _increment_counter()
    if product:
        logger.info("[DB READ]          product id=%d  (total DB reads: %d)", product_id, count)
        return product.to_dict()

    logger.info("[DB READ]          product id=%d  NOT FOUND  (total DB reads: %d)", product_id, count)
    return None
