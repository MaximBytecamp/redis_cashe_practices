from __future__ import annotations

import logging

from app.cache.helpers import cache_delete, cache_delete_many, cache_delete_by_pattern
from app.cache.keys import (
    product_key,
    products_all_key,
    products_by_category_key,
    products_stats_key,
    product_pattern,
)

logger = logging.getLogger("invalidation")

class CacheInvalidationService:
    async def invalidate_product(
        self,
        product_id: int,
        old_category: str | None = None,
        new_category: str | None = None,
    ) -> list[str]:
        """
        Инвалидировать всё, что связано с обновлением одного товара.

        Удаляет:
          - product:{id}
          - products:all
          - products:category:{old_category}
          - products:category:{new_category}  (если категория изменилась)
          - stats:products
        """
        keys_to_delete = [
            product_key(product_id), #proudct:1 
            products_all_key(),      #proudct:all
            products_stats_key(),    #product:stats    #product:catetecory:laptop 
        ]

        if old_category:
            keys_to_delete.append(products_by_category_key(old_category))
        if new_category and new_category != old_category:
            keys_to_delete.append(products_by_category_key(new_category))

        logger.info(
            "INVALIDATE product id=%d  keys=%s",
            product_id, keys_to_delete,
        )
        await cache_delete_many(keys_to_delete)
        return keys_to_delete
    

    async def invalidate_category_batch(
        self,
        category: str,
        product_ids: list[int],
    ) -> list[str]:
        """
        Инвалидировать после массового обновления категории.

        Удаляет:
          - products:category:{category}
          - products:all
          - stats:products
          - product:{id} для каждого затронутого товара
        """
        keys_to_delete = [
            products_by_category_key(category),
            products_all_key(),
            products_stats_key(),
        ]
        for pid in product_ids:
            keys_to_delete.append(product_key(pid))

        logger.info(
            "INVALIDATE BATCH  category=%s  product_count=%d  keys=%d",
            category, len(product_ids), len(keys_to_delete),
        )
        await cache_delete_many(keys_to_delete)
        return keys_to_delete
    


    async def invalidate_all_products(self) -> int:
        """
        Полная инвалидация всех product-ключей через SCAN.
        Используется вместо FLUSHALL — точечная очистка.
        """
        total = 0
        total += await cache_delete_by_pattern(product_pattern()) #product:* -> проходиться частями по 100 элементов redis и искать все ключи
        #вида product:1, product: 12 , ... после 100 пройденных ключей Redis возвращается список найденных мы их удаляем и дальше берем следующие 100
        #и находим product:26, product: 100 -> удаляем и так далее, как только cursor доходит до конца его элементов Redis он становится по значению 0 
        #и сканирование завершается 
        total += await cache_delete_by_pattern("products:*")
        total += await cache_delete("stats:products")
        logger.info("INVALIDATE ALL  deleted=%d", total)
        return total


invalidation_service = CacheInvalidationService()
    


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