"""CacheSyncService — ядро синхронизации кеша.

Три стратегии:
  write_through — после записи в БД сразу пишем новое значение в кеш
  invalidate    — после записи удаляем устаревшие ключи
  hybrid        — write-through для карточки, invalidate для списков/статистики
  none          — кеш не обновляется (для демонстрации stale-данных)
"""

from __future__ import annotations

import logging
from typing import Any

from app.cache import helpers as ch
from app.cache import keys as ck
from app.config import settings

logger = logging.getLogger("cache_sync")


class CacheSyncService:
    """Выбирает стратегию синхронизации кеша после записи в БД."""

    @classmethod
    async def after_product_update(
        cls,
        product_id: int,
        new_data: dict[str, Any],
        old_category: str | None = None,
        new_category: str | None = None,
    ) -> dict[str, Any]:
        """Синхронизирует кеш после обновления одного товара.

        Возвращает словарь с метриками операции:
          strategy, wt_keys, invalidated_keys
        """
        mode = settings.sync_mode
        metrics: dict[str, Any] = {"strategy": mode, "wt_keys": [], "invalidated_keys": []}

        if mode == "none":
            logger.info("[SYNC:NONE] product:%d — кеш не обновлён", product_id)
            return metrics

        if mode == "write_through":
            await cls._write_through_card(product_id, new_data, metrics)
            await cls._write_through_list_all(metrics)
            await cls._write_through_category(new_category or new_data.get("category"), metrics)
            if old_category and old_category != new_category:
                await cls._write_through_category(old_category, metrics)
            await cls._write_through_stats(metrics)

        elif mode == "invalidate":
            await cls._invalidate_card(product_id, metrics)
            await cls._invalidate_derived(old_category, new_category, metrics)

        elif mode == "hybrid":
            # Карточка — write-through (мгновенная консистентность)
            await cls._write_through_card(product_id, new_data, metrics)
            # Списки и статистика — invalidate (дешевле пересчитывать лениво)
            await cls._invalidate_derived(old_category, new_category, metrics)

        logger.info(
            "[SYNC:%s] product:%d  wt=%d  inv=%d",
            mode.upper(), product_id,
            len(metrics["wt_keys"]), len(metrics["invalidated_keys"]),
        )
        return metrics
    
    
    @classmethod
    async def _write_through_card(
        cls, product_id: int, data: dict[str, Any], metrics: dict
    ) -> None:
        key = ck.product_key(product_id)
        await ch.cache_write_through(key, data)
        metrics["wt_keys"].append(key)

    @classmethod
    async def _write_through_list_all(cls, metrics: dict) -> None:
        """Перестраиваем список всех товаров в кеше."""
        from app.repositories.product_repository import ProductRepository
        products = await ProductRepository.get_all()
        key = ck.products_all_key()
        await ch.cache_write_through(key, products)
        metrics["wt_keys"].append(key)
    