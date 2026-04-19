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



