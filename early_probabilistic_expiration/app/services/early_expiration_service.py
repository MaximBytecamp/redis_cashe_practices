"""Early Probabilistic Expiration Service — ядро проекта.

═══════════════════════════════════════════════════════════════════════════
АЛГОРИТМ XFetch (Probabilistic Early Recomputation)
═══════════════════════════════════════════════════════════════════════════

Проблема:
  Если TTL = 120 секунд и 1000 пользователей смотрят товар,
  в момент T=120 ВСЕ увидят cache miss и ВСЕ побегут в БД.
  Это cache stampede.

Идея XFetch:
  Пересчитать данные ЗАРАНЕЕ, ДО истечения TTL.
  Но не жёстко «за 10 секунд до», а ВЕРОЯТНОСТНО:
  чем ближе к expiry — тем выше шанс, что ОДИН из запросов
  сам решит «я пересчитаю данные фоново».

Формула:
  should_recompute = (now - (expiry - delta × β × ln(random()))) > 0

  Где:
    now     — текущее время
    expiry  — когда ключ истечёт (unix timestamp)
    delta   — время последнего вычисления (сколько длился DB read)
    β (beta) — коэффициент агрессивности (обычно 1.0)
    random() — случайное число от 0 до 1
    ln()     — натуральный логарифм

  Раскроем:
    -ln(random()) всегда > 0 (т.к. random() ∈ (0,1], ln < 0, минус на минус)
    Чем ближе now к expiry, тем больше шанс, что выражение > 0.
    Чем больше delta (тяжёлый запрос), тем раньше начнётся пересчёт.
    Чем больше beta, тем агрессивнее (раньше) пересчёт.

Пример:
  TTL = 120s, delta = 0.05s (быстрый запрос), beta = 1.0
  → Пересчёт начнётся примерно за 0.05-0.5с до expiry
  → На быстрых запросах XFetch почти не влияет

  TTL = 120s, delta = 2.0s (тяжёлый запрос), beta = 1.0
  → Пересчёт начнётся примерно за 2-20с до expiry
  → На тяжёлых запросах XFetch даёт большой запас

  TTL = 120s, delta = 2.0s, beta = 3.0
  → Пересчёт начнётся примерно за 6-60с до expiry
  → Агрессивный режим: чаще обновляется, но больше DB reads

Два режима для сравнения:
  1. NO_XFETCH  — обычный cache-aside (ждём полного истечения TTL)
  2. WITH_XFETCH — XFetch (пересчитываем заранее с вероятностью)
"""

from __future__ import annotations

import logging
import math
import random
import time

from app.cache.helpers import cache_get, cache_set, xfetch_get, xfetch_set
from app.cache.keys import product_key, product_meta_key
from app.config import settings
from app.repositories import product_repository

logger = logging.getLogger("xfetch")


class XFetchResult:
    """Результат получения данных с метаинформацией."""

    __slots__ = ("data", "source", "delta", "ttl_remaining", "recomputed", "probability")

    def __init__(
        self,
        data: dict | None,
        source: str,
        delta: float = 0.0,
        ttl_remaining: float = 0.0,
        recomputed: bool = False,
        probability: float = 0.0,
    ):
        self.data = data
        self.source = source
        self.delta = delta
        self.ttl_remaining = ttl_remaining
        self.recomputed = recomputed
        self.probability = probability


def _should_recompute(now: float, expiry: float, delta: float, beta: float) -> tuple[bool, float]:
    """Вычислить, нужно ли пересчитывать данные (формула XFetch).

    Возвращает (should_recompute, gap):
      gap — разница: сколько секунд «перевешивает» в сторону пересчёта.
            gap > 0 → пересчитываем, gap < 0 → ещё рано.
    """
    # -ln(random()) даёт экспоненциальное распределение со средним = 1
    rand_val = random.random()
    if rand_val == 0:
        rand_val = 1e-10  # защита от log(0)

    gap = now - (expiry - delta * beta * (-math.log(rand_val)))

    logger.info(
        "[XFETCH CALC]      delta=%.4f  beta=%.1f  -ln(%.4f)=%.4f  gap=%.4f  ttl_remaining=%.1fs  → %s",
        delta, beta, rand_val, -math.log(rand_val), gap,
        expiry - now,
        "RECOMPUTE" if gap > 0 else "KEEP",
    )

    return gap > 0, gap


async def get_product_no_xfetch(product_id: int) -> XFetchResult:
    """Обычный cache-aside БЕЗ XFetch — ждём полного истечения TTL."""
    key = product_key(product_id)

    cached = await cache_get(key)
    if cached is not None:
        return XFetchResult(data=cached, source="cache")

    # Cache miss → идём в БД
    data, delta = await product_repository.get_product_by_id(product_id)
    if data is not None:
        await cache_set(key, data)
    return XFetchResult(data=data, source="db_direct", delta=delta)


async def get_product_with_xfetch(product_id: int) -> XFetchResult:
    """Cache-aside С XFetch — вероятностный пересчёт ДО истечения TTL."""
    key = product_key(product_id)
    meta_key = product_meta_key(product_id)

    # Читаем данные + метаданные одним вызовом
    data, delta, expiry = await xfetch_get(key, meta_key)

    if data is None:
        # Полный cache miss → обязательно идём в БД
        logger.info("[XFETCH]           %s  FULL MISS → DB read", key)
        db_data, db_delta = await product_repository.get_product_by_id(product_id)
        if db_data is not None:
            await xfetch_set(key, meta_key, db_data, db_delta)
        return XFetchResult(
            data=db_data, source="db_miss", delta=db_delta, recomputed=True,
        )

    # Данные есть — проверяем, не пора ли пересчитать
    now = time.time()
    ttl_remaining = expiry - now

    if expiry <= 0 or delta <= 0:
        # Нет метаданных XFetch — просто возвращаем кеш
        return XFetchResult(
            data=data, source="cache", ttl_remaining=ttl_remaining,
        )

    should, gap = _should_recompute(now, expiry, delta, settings.xfetch_beta)

    if should:
        # Вероятность сработала — пересчитываем ФОНОВО
        # (в реальном production это делается в background task,
        #  но для наглядности — прямо в запросе)
        logger.info(
            "[XFETCH RECOMPUTE] %s  ttl_remaining=%.1fs  gap=%.4f → обновляем кеш",
            key, ttl_remaining, gap,
        )
        db_data, db_delta = await product_repository.get_product_by_id(product_id)
        if db_data is not None:
            await xfetch_set(key, meta_key, db_data, db_delta)
            return XFetchResult(
                data=db_data, source="xfetch_recompute",
                delta=db_delta, ttl_remaining=ttl_remaining,
                recomputed=True, probability=gap,
            )

    # Вероятность не сработала — возвращаем кешированные данные
    return XFetchResult(
        data=data, source="cache_xfetch",
        delta=delta, ttl_remaining=ttl_remaining,
        recomputed=False, probability=gap,
    )
