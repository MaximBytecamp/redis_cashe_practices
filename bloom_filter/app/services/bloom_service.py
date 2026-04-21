from __future__ import annotations

import logging
import math

import mmh3

from app.cache.redis_client import get_redis
from app.config import settings

logger = logging.getLogger("bloom")


def _optimal_params(n: int, p: float) -> tuple[int, int]:
    """Рассчитать оптимальные m (бит) и k (хешей)."""
    m = int(-n * math.log(p) / (math.log(2) ** 2))
    k = max(1, int((m / max(n, 1)) * math.log(2)))
    return m, k


# Глобальные параметры
_M, _K = _optimal_params(settings.bloom_expected_items, settings.bloom_fp_rate)

# Статистика
_stats: dict[str, int] = {
    "checks": 0,
    "definite_no": 0,    # точно нет — фильтр отсёк
    "maybe_yes": 0,      # возможно есть — пропустили дальше
    "adds": 0,
}


def get_bloom_stats() -> dict[str, int]:
    return {**_stats, "m_bits": _M, "k_hashes": _K}


def reset_bloom_stats() -> None:
    for k in ("checks", "definite_no", "maybe_yes", "adds"):
        _stats[k] = 0


def _hash_positions(item: str) -> list[int]:
    """Вычислить k позиций бита для элемента."""
    positions = []
    for seed in range(_K):
        h = mmh3.hash(item, seed, signed=False)
        positions.append(h % _M)
    return positions


async def bloom_add(product_id: int) -> None:
    """Добавить product_id в Bloom filter."""
    r = await get_redis()
    key = settings.bloom_key
    positions = _hash_positions(str(product_id))

    pipe = r.pipeline()
    for pos in positions:
        pipe.setbit(key, pos, 1)
    await pipe.execute()

    _stats["adds"] += 1
    logger.info("[BLOOM ADD]       product:%d  positions=%s", product_id, positions[:3])


async def bloom_check(product_id: int) -> bool:
    r = await get_redis()
    key = settings.bloom_key
    positions = _hash_positions(str(product_id))

    pipe = r.pipeline()
    for pos in positions:
        pipe.getbit(key, pos)
    bits = await pipe.execute()

    _stats["checks"] += 1

    if all(b == 1 for b in bits):
        _stats["maybe_yes"] += 1
        logger.info("[BLOOM MAYBE]     product:%d", product_id)
        return True

    _stats["definite_no"] += 1
    logger.info("[BLOOM NO]        product:%d  (bit %s = 0)", product_id,
                [p for p, b in zip(positions, bits) if b == 0][:2])
    return False


async def bloom_populate(product_ids: list[int]) -> int:
    """Массовая загрузка ID в Bloom filter (при старте)."""
    r = await get_redis()
    key = settings.bloom_key

    # Очищаем старый фильтр
    await r.delete(key)

    pipe = r.pipeline()
    for pid in product_ids:
        for pos in _hash_positions(str(pid)):
            pipe.setbit(key, pos, 1)
    await pipe.execute()

    _stats["adds"] += len(product_ids)
    logger.info("[BLOOM POPULATE]  %d items loaded, m=%d bits, k=%d hashes",
                len(product_ids), _M, _K)
    return len(product_ids)


async def bloom_clear() -> None:
    """Удалить Bloom filter из Redis."""
    r = await get_redis()
    await r.delete(settings.bloom_key)
    logger.info("[BLOOM CLEAR]")


async def bloom_memory_bytes() -> int:
    """Размер Bloom filter в Redis (байты)."""
    r = await get_redis()
    try:
        mem = await r.memory_usage(settings.bloom_key)
        return mem or 0
    except Exception:
        return _M // 8  # приблизительно