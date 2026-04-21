"""Product Service — бизнес-логика получения товара.

Выбирает стратегию (no_protection / mutex_lock) на основе:
  1. параметра `protection` в запросе
  2. глобальной настройки settings.stampede_protection_enabled
"""

from __future__ import annotations

import logging

from app.config import settings
from app.services.stampede_protection_service import (
    StampedeResult,
    get_product_no_protection,
    get_product_with_mutex,
)

logger = logging.getLogger("service")


async def get_product(
    product_id: int, protection: bool | None = None
) -> StampedeResult:
    """Получить товар с выбранной стратегией.

    Args:
        product_id: ID товара
        protection: True/False явно, или None → берём из settings
    """
    use_protection = (
        protection if protection is not None else settings.stampede_protection_enabled
    )

    if use_protection:
        logger.info("[STRATEGY]         MUTEX_LOCK  product_id=%d", product_id)
        return await get_product_with_mutex(product_id)
    else:
        logger.info("[STRATEGY]         NO_PROTECTION  product_id=%d", product_id)
        return await get_product_no_protection(product_id)
