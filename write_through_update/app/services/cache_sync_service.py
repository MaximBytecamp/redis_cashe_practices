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

from app.repositories.product_repository import ProductRepository

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
    async def after_batch_update(
        cls,
        category: str,
        updated_products: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Синхронизация после массового обновления цен."""
        mode = settings.sync_mode
        metrics: dict[str, Any] = {"strategy": mode, "wt_keys": [], "invalidated_keys": []}

        if mode == "none":
            return metrics

        if mode in ("write_through", "hybrid"):
            # Write-through для каждой карточки
            for p in updated_products:
                await cls._write_through_card(p["id"], p, metrics)

        if mode in ("invalidate", "hybrid"):
            # Invalidate списки и стат
            keys_to_del = [
                ck.products_all_key(),
                ck.products_category_key(category),
                ck.products_stats_key(),
            ]
            await ch.cache_delete_many(keys_to_del)
            metrics["invalidated_keys"].extend(keys_to_del)
        elif mode == "write_through":
            # Pure WT — обновляем списки тоже
            await cls._write_through_category(category, metrics)
            await cls._write_through_list_all(metrics)
            await cls._write_through_stats(metrics)

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
        products = await ProductRepository.get_all()
        key = ck.products_all_key()
        await ch.cache_write_through(key, products)
        metrics["wt_keys"].append(key)

    @classmethod
    async def _write_through_category(
        cls, category: str | None, metrics: dict
    ) -> None:
        if not category:
            return
        
        products = await ProductRepository.get_by_category(category)
        key = ck.products_category_key(category)
        await ch.cache_write_through(key, products)
        metrics["wt_keys"].append(key)

    @classmethod
    async def _write_through_stats(cls, metrics: dict) -> None:
        stats = await ProductRepository.get_stats()
        key = ck.products_stats_key()
        await ch.cache_write_through(key, stats)
        metrics["wt_keys"].append(key)


    @classmethod
    async def _invalidate_card(cls, product_id: int, metrics: dict) -> None:
        key = ck.product_key(product_id)
        await ch.cache_delete(key)
        metrics["invalidated_keys"].append(key)

    @classmethod
    async def _invalidate_derived(
        cls,
        old_category: str | None,
        new_category: str | None,
        metrics: dict,
    ) -> None:
        keys_to_del = [ck.products_all_key(), ck.products_stats_key()]
        if new_category:
            keys_to_del.append(ck.products_category_key(new_category))
        if old_category and old_category != new_category:
            keys_to_del.append(ck.products_category_key(old_category))
        await ch.cache_delete_many(keys_to_del)
        metrics["invalidated_keys"].extend(keys_to_del)


#ОБНОВЛЯЕМ БАЗЫ ДАННЫХ (CASHE ASIDE -> необходимость синхронизировать актуальные данные с Redis) и есть два подхода это Invalidation и 
#Write Throught (update) -> price из 10 становится 15 (product:5) и получается что в redis под product:5 хранится price с неактуальными
#данными и мы должны значит ключ из redis:5 удалить -> я из redis удаляю сам ключ product:5, далее удаляю ключ products:all который
#хранил список всех моих товаров (удаляем потому что product:5 уже имеет другую цену и этот ключ тоже не актулаен) и удаляем ключ
#prudct:stats потому что в нем хранится актуальная статистика по всем товарам но так ккак в бд обновился product:5 с ценой то статистика в этом
#ключ уже не актуальна. -> получается что пользователь потом делаем product:stats -> CASHE MISS -> идет в бд там статистика актуализируется
#и актуальная статистика записывается в ключ product:stats -> и пользователь потом с ней постоянно работает 

#ДОПОЛНЕНИЕ (ПО-ХОРОШЕМУ К СЛУЧАЕ ВЫШЕ МЫ ДОЛЖНЫ БЫЛИ ПЕРЕДАТЬ ЕЩЕ old_category) это его category товара зачем?
#потому что в Redis у нас есть ключ который хранит по конкретной категории список всех связанных с ним товаров и по-хорошему этот ключ
#мы тоже должны удалить потому что там находится товар product:5 который изменил цену и при взятие ключа product:category.. будут возвращаться
#всет овары и среди них будет product:5 с неактуальными данными 

#СЛУЧАЙ 2. когда мы обновляем в бд product:5 его категорию то есть был laptop -> monitor. и в чем заключается проблема так товар изменился
#то мы должны также как и в первом случае удалить все связанные с этим товаром ключи потому что они уже хранят неактуальную информацию 

#но и есть инетересный вариант получается что сейчас В СТАРОЙ. КАТЕГОРИИ laptop находится product:5 И В НОВОЙ КАТЕГОРИИ КОТОРУЮ ОН ПЕРЕШЕЛ в
#среди списков товара его нет в Redis. Мы должны инвалидировать из Redis ключи product:category:laptop и prudct:category:monitor иначе
#при взятие ключа monitor мы будем получать неактуальные данные потому что product:5 как бы в бд уже в этой категории но в Redis еще нет
#и laptop удалить потому что при взятии мы будем получать неактуальные данные так как product:5 уже не в этой категории но мы его все еще видим



#REDIS -> product:category:laptop = [5, 6, 7 ,8]        ->  [5, 7, 8]
#          product:category:monitor = [4, 9, 12, 13]    ->  [4, 9, 12, 6, 13]