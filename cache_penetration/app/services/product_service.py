from __future__ import annotations

import logging
from typing import Any

from app.repositories.product_repository import ProductRepository
from app.services.cache_service import CacheService

logger = logging.getLogger("product_service")


_counters: dict[str, int] = {
    "cache_hit": 0,
    "null_hit": 0,
    "cache_miss": 0,
    "db_read": 0,
    "db_read_found": 0,
    "db_read_not_found": 0,
}


def reset_counters() -> None:
    for k in _counters:
        _counters[k] = 0


def get_counters() -> dict[str, int]:
    return dict(_counters)


class ProductService:
    """Высокоуровневый сервис: cache-aside + null caching."""

    # ── READ

    @staticmethod
    async def get_product(product_id: int) -> dict[str, Any]:

        # Шаг 1: проверить кеш
        cr = await CacheService.get_product(product_id)

        if cr.state == "hit":
            _counters["cache_hit"] += 1
            return {"data": cr.data, "_source": "cache", "_status": 200}

        if cr.state == "null_hit":
            _counters["null_hit"] += 1
            return {"data": cr.data, "_source": "null_cache", "_status": 404}

        # Шаг 2: cache miss и идём в БД
        _counters["cache_miss"] += 1
        _counters["db_read"] += 1
        data = await ProductRepository.get_by_id(product_id)

        if data is not None:
            _counters["db_read_found"] += 1
            await CacheService.set_product(product_id, data)
            return {"data": data, "_source": "db", "_status": 200}

        # Не найден то null cache
        _counters["db_read_not_found"] += 1
        await CacheService.set_null(product_id)
        return {"data": None, "_source": "db_not_found", "_status": 404}
    
    # ── CREATE 

    @staticmethod
    async def create_product(data: dict[str, Any], product_id: int | None = None) -> dict[str, Any]:
        """Создать товар. Перезаписать null marker если был."""
        if product_id is not None:
            result = await ProductRepository.create_with_id(product_id, data)
        else:
            result = await ProductRepository.create(data)
        # Записать в кеш поверх возможного null marker
        await CacheService.set_product(result["id"], result)
        return {"data": result, "_source": "created", "_status": 201}
    

    # ── UPDATE 

    @staticmethod
    async def update_product(product_id: int, data: dict[str, Any]) -> dict[str, Any]:
        result = await ProductRepository.update(product_id, data)
        if result is not None:
            await CacheService.set_product(product_id, result)
            return {"data": result, "_source": "updated", "_status": 200}
        
        await CacheService.set_null(product_id)
        return {"data": None, "_source": "not_found", "_status": 404}
    
    # ── DELETE

    @staticmethod
    async def delete_product(product_id: int, strategy: str = "write_null") -> dict[str, Any]:
        """
        strategy:
          'delete_only'  — только удалить ключ Redis
          'write_null'   — записать null marker после удаления
        """
        deleted = await ProductRepository.delete(product_id)
        if not deleted:
            return {"deleted": False, "_source": "not_found", "_status": 404, "strategy": strategy}

        if strategy == "write_null":
            # Стратегия B: сразу записать null marker
            await CacheService.invalidate(product_id)
            await CacheService.set_null(product_id)
        else:
            # Стратегия A: просто удалить ключ
            await CacheService.invalidate(product_id)

        return {"deleted": True, "_source": "deleted", "_status": 200, "strategy": strategy}
    
    # ── DEBUG 

    @staticmethod
    async def get_cache_debug(product_id: int) -> dict[str, Any]:
        cache_info = await CacheService.get_debug(product_id)
        db_val = await ProductRepository.get_by_id(product_id)
        return {
            "product_id": product_id,
            "cache": cache_info,
            "database": db_val,
        }


