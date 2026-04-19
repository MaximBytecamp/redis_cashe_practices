from __future__ import annotations

import logging
import math

import mmh3

from app.cache.redis_client import get_redis
from app.config import settings

logger = logging.getLogger("bloom")

"""Bloom Filter на базе Redis SETBIT/GETBIT.

Bloom filter — вероятностная структура данных:
  - "Точно НЕТ в множестве"   → гарантия 100%
  - "Возможно ЕСТЬ в множестве" → с вероятностью ложного срабатывания (FP rate)

Реализация:
  - Биты хранятся в Redis key (строка-bitmap) — общий для всех процессов
  - k хеш-функций = k позиций бита для каждого элемента
  - Используем mmh3 (MurmurHash3) с разными seed для k хешей

Формулы:
  m = -(n * ln(p)) / (ln(2))^2         — размер фильтра в битах
  k = (m / n) * ln(2)                   — оптимальное число хешей

  n = ожидаемое число элементов
  p = допустимая вероятность ложного срабатывания
"""

def _optimal_params(n: int, p: float) -> tuple[int, int]:
    """Рассчитать оптимальные m (бит) и k (хешей)."""
    m = int(-n * math.log(p) / (math.log(2) ** 2))
    k = max(1, int((m / max(n, 1)) * math.log(2)))
    return m, k



