from __future__ import annotations

import json
import logging
import time
from typing import Any

from app.cache.redis_client import get_redis
from app.config import settings

logger = logging.getLogger("cache")


#Стандартные кэш-операции

async def cache_get(key: str) -> dict | None:
    """Прочитать ключ из Redis. Возвращает dict или None."""
    r = await get_redis()
    raw = await r.get(key)

    if raw is None:
        logger.info("[CACHE MISS]       %s", key)
        return None

    logger.info("[CACHE HIT]        %s", key)
    return json.loads(raw)


async def cache_set(key: str, value: Any, ttl: int | None = None) -> None:
    """Записать данные в кеш с TTL."""
    r = await get_redis()
    ttl = ttl or settings.product_cache_ttl
    await r.set(key, json.dumps(value, default=str), ex=ttl)
    logger.info("[CACHE SET]        %s  ttl=%ds", key, ttl)


async def cache_delete(key: str) -> int:
    """Удалить ключ из кеша."""
    r = await get_redis()
    deleted = await r.delete(key)
    if deleted:
        logger.info("[CACHE DELETE]     %s", key)
    return deleted


#XFetch-расширенные операции

async def xfetch_set(
    key: str,
    meta_key: str,
    value: Any,
    delta: float,
    ttl: int | None = None,
) -> None:
    """Записать данные + метаданные XFetch (delta, expiry) в Redis.

    key      — ключ данных (product:1)
    meta_key — ключ метаданных (meta:product:1)
    value    — данные для кеша (dict)
    delta    — время вычисления в секундах (сколько длился DB read)
    ttl      — время жизни ключа в секундах
    """
    r = await get_redis()
    ttl = ttl or settings.product_cache_ttl
    expiry = time.time() + ttl

    # Записываем данные с TTL
    await r.set(key, json.dumps(value, default=str), ex=ttl)

    # Записываем метаданные (delta + expiry) с тем же TTL + запас
    meta = json.dumps({"delta": delta, "expiry": expiry})
    await r.set(meta_key, meta, ex=ttl + 60)  # +60с запас чтобы мета не исчезла раньше данных

    logger.info(
        "[XFETCH SET]       %s  ttl=%ds  delta=%.4fs  expiry=%.1f",
        key, ttl, delta, expiry,
    )


async def xfetch_get(key: str, meta_key: str) -> tuple[dict | None, float, float]:
    """Прочитать данные + метаданные XFetch из Redis.

    Возвращает (data, delta, expiry):
      data   — dict с данными или None (cache miss)
      delta  — время вычисления (0.0 если мета нет)
      expiry — время истечения unix timestamp (0.0 если мета нет)
    """
    r = await get_redis()

    # Читаем данные и мету одним pipeline для эффективности
    pipe = r.pipeline()
    pipe.get(key)
    pipe.get(meta_key)
    raw_data, raw_meta = await pipe.execute()

    if raw_data is None:
        logger.info("[CACHE MISS]       %s", key)
        return None, 0.0, 0.0

    data = json.loads(raw_data)

    if raw_meta is not None:
        meta = json.loads(raw_meta)
        delta = meta.get("delta", 0.0)
        expiry = meta.get("expiry", 0.0)
        logger.info(
            "[XFETCH META]      %s  delta=%.4f  expiry=%.1f  remaining=%.1fs",
            key, delta, expiry, expiry - time.time(),
        )
    else:
        delta = 0.0
        expiry = 0.0
        logger.info("[CACHE HIT]        %s  (no xfetch meta)", key)

    return data, delta, expiry


async def get_ttl(key: str) -> int:
    """Получить оставшийся TTL ключа в секундах (-1 = нет TTL, -2 = не существует)."""
    r = await get_redis()
    return await r.ttl(key)
