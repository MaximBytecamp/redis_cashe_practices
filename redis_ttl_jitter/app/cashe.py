from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import time
from typing import Any

from app.config import settings
from app.database import fetch_product_from_db
from app.metrics import metrics
from app.redis_client import get_redis


logger = logging.getLogger("cache")

TTL_TABLE: dict[str, tuple(int, int)] = {
    "hot": (120, 180),
    ""
    "": (60, 80),
    "rare": (30, 50)
}

def get_ttl(base: int | None = None, jitter: int | None = None) -> int:
    if base is None:
        base = settings.base_ttl

    if jitter is None:
        jitter = settings.jitter_max

    return base + random.randint(0, jitter) #60 + (случаное от 0 до jitter) -> 


def get_ttl_no_jitter(base: int | None = None) -> int:
    if base is None:
        base = settings.base_ttl

    return base 


def get_ttl_by_category(category: str) -> int:
    lo, hi = TTL_TABLE.get(category, (60,80))

    return random.randint(lo, hi) #генерирует случайное число из диапазона от 120 до 180 и это значение присваивает ключу на время жизни


#  • Local in-memory cache — снимает нагрузку даже с Redis
class LocalCache:
    def __init__(self, default_ttl:float=5.0):
        self._store: dict[str, tuple[Any, float]] = {}
        self._default_ttl = default_ttl

    def get(self, key: str) -> Any | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        if time.monotonic() > expires_at: 
            del self._store[key]
            return None
        return value


    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        if ttl is None:
            ttl = self._default_ttl
        self._store[key] = (value, time.monotonic() + ttl)


    def clear(self) -> None:
        self._store.clear()


local_cache = LocalCache(default_ttl=5.0)



#из Redis -> добавить ключ + значенив кеш l1 (localcashe) чтобы мгновенно возвращать результаты в течение указанного жизни ключа
#key=name, value=maxim, ttl = 10 -> _store[name] = (maxim, 100000 + 10 (100010))

#get -> _store[name] -> entry = (maxim, 1000010) -> value=maxim, expires_at=1000010, 1000011 > 1000010


#   • Probabilistic (вероятностное) Early (раннее) Expiration — обновление кэша ДО истечения TTL

def should_early_recompute(ttl_remaining: int, beta: float = 1.0) -> bool:
    """
    XFetch / Probabilistic early expiration.
    Чем меньше TTL осталось — тем выше шанс перевычислить заранее.

    P(recompute) = exp(-ttl_remaining / beta)
    """
    if ttl_remaining <= 0:
        return True
    probability = math.exp(-ttl_remaining / beta)
    return random.random() < probability


_use_jitter: bool = True
_use_mutex: bool = True
_use_null_cache: bool = True
_use_local_cache: bool = True
_use_early_expiration: bool = False


LOCK_PREFIX = "lock:"
LOCK_TTL = 5          # секунд
LOCK_WAIT_STEP = 0.05 # пауза между попытками
LOCK_MAX_WAIT = 3.0   # макс. ожидание


def configure_cache(
    *,
    use_jitter: bool | None = None,
    use_mutex: bool | None = None,
    use_null_cache: bool | None = None,
    use_local_cache: bool | None = None,
    use_early_expiration: bool | None = None) -> dict[str, bool]:
    global _use_jitter, _use_mutex, _use_null_cache, _use_local_cache, _use_early_expiration
    if use_jitter is not None:
        _use_jitter = use_jitter
    if use_mutex is not None:
        _use_mutex = use_mutex
    if use_null_cache is not None:
        _use_null_cache = use_null_cache
    if use_local_cache is not None:
        _use_local_cache = use_local_cache
    if use_early_expiration is not None:
        _use_early_expiration = use_early_expiration
    return current_config()


def current_config() -> dict[str, bool]:
    return {
        "use_jitter": _use_jitter,
        "use_mutex": _use_mutex,
        "use_null_cache": _use_null_cache,
        "use_local_cache": _use_local_cache,
        "use_early_expiration": _use_early_expiration,
    }


async def get_product(product_id: int) -> dict | None:
    """
    Получить продукт: Local Cache → Redis → DB.

    Полная цепочка:
      1. Проверить local cache (hot key optimization)
      2. Проверить Redis
         - early expiration (если включено)
      3. Если miss → mutex lock (если включено) → DB
      4. Null caching если продукт не найден
      5. Записать в Redis + local cache
    """
    cache_key = f"product:{product_id}"

    # ── 1. Local cache ─────────────────────────────────────
    if _use_local_cache:
        local_val = local_cache.get(cache_key)
        if local_val is not None:
            metrics.hit("local")
            metrics.hit("cache")
            return local_val if local_val != "__null__" else None

    # ── 2. Redis ───────────────────────────────────────────
    r = await get_redis()
    cached = await r.get(cache_key)

    if cached is not None:
        # Null caching: мы храним строку "null"
        if cached == "null":
            metrics.hit("null_cache")
            metrics.hit("cache")
            if _use_local_cache:
                local_cache.set(cache_key, "__null__", ttl=3.0)
            return None

        data = json.loads(cached)

        # Probabilistic early expiration
        if _use_early_expiration:
            ttl_rem = await r.ttl(cache_key)
            if should_early_recompute(ttl_rem, beta=2.0):
                logger.info("Early recompute triggered for %s (ttl_remaining=%s)", cache_key, ttl_rem)
                # Не блокируем — обновим в фоне, вернём старые данные
                asyncio.create_task(_refresh_cache(product_id, cache_key))

        metrics.hit("cache")
        if _use_local_cache:
            local_cache.set(cache_key, data, ttl=5.0)
        return data

    # ── 3. Cache miss ──────────────────────────────────────
    metrics.cache_misses += 1

    if _use_mutex:
        return await _fetch_with_lock(product_id, cache_key)
    else:
        return await _fetch_and_cache(product_id, cache_key)


async def _fetch_with_lock(product_id: int, cache_key: str) -> dict | None:
    """
    Anti-Stampede: только один запрос идёт в БД,
    остальные ждут пока кэш прогреется.
    """
    r = await get_redis()
    lock_key = LOCK_PREFIX + cache_key

    # Попытка захватить блокировку
    acquired = await r.set(lock_key, "1", nx=True, ex=LOCK_TTL)

    if acquired:
        try:
            return await _fetch_and_cache(product_id, cache_key)
        finally:
            await r.delete(lock_key)
    else:
        # Ждём пока владелец лока запишет данные
        metrics.hit("lock_wait")
        waited = 0.0
        while waited < LOCK_MAX_WAIT:
            await asyncio.sleep(LOCK_WAIT_STEP)
            waited += LOCK_WAIT_STEP
            cached = await r.get(cache_key)
            if cached is not None:
                if cached == "null":
                    return None
                data = json.loads(cached)
                metrics.hit("cache")
                return data

        # Таймаут — идём в БД сами
        return await _fetch_and_cache(product_id, cache_key)


async def _fetch_and_cache(product_id: int, cache_key: str) -> dict | None:
    """Достать из БД и записать в кэш."""
    data = await fetch_product_from_db(product_id)
    metrics.hit("db")
    r = await get_redis()

    if data is None:
        # Null caching
        if _use_null_cache:
            null_ttl = random.randint(5, 15)
            await r.set(cache_key, "null", ex=null_ttl)
            logger.info("SET %s = null  ttl=%d (null-cache)", cache_key, null_ttl)
            metrics.record_ttl(null_ttl)
        return None

    # Определяем TTL
    category = data.get("category", "normal")
    if _use_jitter:
        ttl = get_ttl_by_category(category)
    else:
        ttl = get_ttl_no_jitter()

    value = json.dumps(data)
    await r.set(cache_key, value, ex=ttl)
    logger.info("SET %s  ttl=%d  jitter=%s  category=%s", cache_key, ttl, _use_jitter, category)
    metrics.record_ttl(ttl)

    if _use_local_cache:
        local_cache.set(cache_key, data, ttl=5.0)

    return data


async def _refresh_cache(product_id: int, cache_key: str) -> None:
    """Фоновое обновление кэша (early expiration)."""
    try:
        await _fetch_and_cache(product_id, cache_key)
    except Exception as exc:
        logger.warning("Background refresh failed for %s: %s", cache_key, exc)