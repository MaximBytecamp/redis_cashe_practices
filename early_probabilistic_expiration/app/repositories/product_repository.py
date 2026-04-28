"""Репозиторий товаров — прямой доступ к БД.

Все обращения к БД логируются как [DB READ].
Поддерживает имитацию медленных запросов через db_simulate_delay.
Содержит атомарный счётчик db_read_count для нагрузочных тестов.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time

from sqlalchemy import select

from app.config import settings
from app.db import async_session
from app.models.product import Product

logger = logging.getLogger("repository")

# ── Атомарный счётчик DB reads (thread-safe) ──
_lock = threading.Lock()
_db_read_count: int = 0

# ── Переопределяемая задержка (для тестовых сценариев) ──
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


async def get_product_by_id(product_id: int) -> tuple[dict | None, float]:
    """Прочитать товар из БД. Возвращает (dict | None, delta).

    delta — время выполнения запроса в секундах (нужно для XFetch).
    """
    delay = _simulate_delay
    if delay > 0:
        logger.info("[DB DELAY]         product id=%d  sleeping %.2fs", product_id, delay)
        await asyncio.sleep(delay)

    t0 = time.perf_counter()

    async with async_session() as session:
        result = await session.execute(
            select(Product).where(Product.id == product_id)
        )
        product = result.scalar_one_or_none()

    delta = time.perf_counter() - t0
    count = _increment_counter()

    if product:
        logger.info(
            "[DB READ]          product id=%d  delta=%.4fs  (total DB reads: %d)",
            product_id, delta, count,
        )
        return product.to_dict(), delta

    logger.info(
        "[DB READ]          product id=%d  NOT FOUND  delta=%.4fs  (total DB reads: %d)",
        product_id, delta, count,
    )
    return None, delta
