"""Product Service — бизнес-логика получения товара."""

from __future__ import annotations

import logging

from app.config import settings
from app.services.early_expiration_service import (
    XFetchResult,
    get_product_no_xfetch,
    get_product_with_xfetch,
)

logger = logging.getLogger("service")


async def get_product(
    product_id: int, xfetch: bool | None = None
) -> XFetchResult:
    use_xfetch = xfetch if xfetch is not None else settings.xfetch_enabled

    if use_xfetch:
        logger.info("[STRATEGY]         XFETCH  product_id=%d", product_id)
        return await get_product_with_xfetch(product_id)
    else:
        logger.info("[STRATEGY]         NO_XFETCH  product_id=%d", product_id)
        return await get_product_no_xfetch(product_id)
