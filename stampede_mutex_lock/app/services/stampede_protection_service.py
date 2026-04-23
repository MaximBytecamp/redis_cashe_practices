
from __future__ import annotations

import asyncio
import logging
import uuid

from app.cache.helpers import cache_get, cache_set, lock_acquire, lock_release
from app.cache.keys import product_key, product_lock_key
from app.config import settings
from app.repositories import product_repository

logger = logging.getLogger("stampede")


class StampedeResult:
    __slots__ = ("data", "source", "retries_used", "lock_owner")

    def __init__(
        self,
        data: dict | None,
        source: str,
        retries_used: int = 0,
        lock_owner: str | None = None,
    ):
        self.data = data
        self.source = source            # "cache" | "db_via_lock" | "cache_after_retry" | "fallback"
        self.retries_used = retries_used
        self.lock_owner = lock_owner


# Режим 1: без защиты (обычный cache-aside)

async def get_product_no_protection(product_id: int) -> StampedeResult:
    """Обычный cache-aside БЕЗ mutex lock."""
    key = product_key(product_id)

    # 1. Проверяем кэш
    cached = await cache_get(key)
    if cached is not None:
        return StampedeResult(data=cached, source="cache")

    # 2. Cache miss → сразу в БД (все запросы пойдут параллельно!)
    data = await product_repository.get_product_by_id(product_id)
    if data is not None:
        await cache_set(key, data)
    return StampedeResult(data=data, source="db_direct")


# Режим 2: с mutex lock + double-check + retry + fallback

async def get_product_with_mutex(product_id: int) -> StampedeResult:
    key = product_key(product_id)
    lk = product_lock_key(product_id)

    # Шаг 1: первая проверка кэша
    cached = await cache_get(key)
    if cached is not None:
        return StampedeResult(data=cached, source="cache")

    # Шаг 2: попытка взять lock
    owner = uuid.uuid4().hex[:12]
    acquired = await lock_acquire(lk, owner)

    if acquired:
        try:
            # Double-check: пока мы брали lock, другой поток мог уже
            # записать данные в Redis
            cached_again = await cache_get(key)
            if cached_again is not None:
                logger.info(
                    "[DOUBLE-CHECK HIT] %s  (другой поток уже записал)", key
                )
                return StampedeResult(
                    data=cached_again,
                    source="cache_double_check",
                    lock_owner=owner,
                )

            # Идём в БД
            data = await product_repository.get_product_by_id(product_id)
            if data is not None:
                await cache_set(key, data)
            return StampedeResult(
                data=data, source="db_via_lock", lock_owner=owner
            )
        finally:
            await lock_release(lk, owner)
    else:
        return await _retry_loop(product_id, key, lk)


async def _retry_loop(product_id: int, key: str, lk: str) -> StampedeResult:
    delay_sec = settings.lock_retry_delay_ms / 1000.0
    max_retries = settings.lock_max_retries

    for attempt in range(1, max_retries + 1):
        logger.info(
            "[RETRY WAIT]       %s  attempt=%d/%d  delay=%.0fms",
            key, attempt, max_retries, settings.lock_retry_delay_ms,
        )
        await asyncio.sleep(delay_sec)

        # Повторная проверка кэша
        cached = await cache_get(key)
        if cached is not None:
            logger.info(
                "[RETRY SUCCESS]    %s  данные появились на попытке %d",
                key, attempt,
            )
            return StampedeResult(
                data=cached, source="cache_after_retry", retries_used=attempt
            )

    # Все retry исчерпаны
    # Вариант B: попробуем сами взять lock (другой владелец мог упасть)
    owner = uuid.uuid4().hex[:12]
    acquired = await lock_acquire(lk, owner)
    if acquired:
        try:
            # Ещё раз double-check
            cached_final = await cache_get(key)
            if cached_final is not None:
                return StampedeResult(
                    data=cached_final,
                    source="cache_after_retry",
                    retries_used=max_retries,
                    lock_owner=owner,
                )
            # Последний шанс — идём в БД
            data = await product_repository.get_product_by_id(product_id)
            if data is not None:
                await cache_set(key, data)
            logger.info("[RETRY DB]         %s  fallback DB read after retries", key)
            return StampedeResult(
                data=data,
                source="db_via_retry_lock",
                retries_used=max_retries,
                lock_owner=owner,
            )
        finally:
            await lock_release(lk, owner)

    # Вариант A: controlled fallback
    logger.warning("[FALLBACK]         %s  reason=lock_timeout", key)
    return StampedeResult(
        data=None, source="fallback", retries_used=max_retries
    )
