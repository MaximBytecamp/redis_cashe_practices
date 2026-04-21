from __future__ import annotations

import logging
from typing import Any

from app.cache import helpers as ch
from app.config import settings
from app.repositories.product_repository import ProductRepository
from app.services.bloom_service import bloom_add, bloom_check

logger = logging.getLogger("product_service")

_counters: dict[str, int] = {
    "bloom_reject": 0,      # отсечено Bloom filter (точно нет)
    "bloom_pass": 0,        # пропущено Bloom filter (возможно есть)
    "neg_hit": 0,           # negative cache hit
    "cache_hit": 0,         # normal cache hit
    "cache_miss": 0,        # cache miss - DB
    "db_read": 0,           # DB read total
    "db_found": 0,          # DB - найден
    "db_not_found": 0,      # DB - не найден (false positive bloom)
}


def reset_counters() -> None:
    for k in _counters:
        _counters[k] = 0


def get_counters() -> dict[str, int]:
    return dict(_counters)


class ProductService:
    """Трёхуровневая защита: Bloom → Negative Cache → Normal Cache → DB."""

    @staticmethod
    async def get_product(product_id: int) -> dict[str, Any]:
        layers: list[str] = []

        # ── Уровень 1: Bloom Filter
        if settings.bloom_enabled:
            maybe_exists = await bloom_check(product_id)
            layers.append(f"bloom:{'maybe' if maybe_exists else 'no'}")

            if not maybe_exists:
                _counters["bloom_reject"] += 1
                return {
                    "data": None,
                    "_source": "bloom_reject",
                    "_status": 404,
                    "_layers": layers,
                }
            _counters["bloom_pass"] += 1

        # ── Уровень 2: Negative Cache
        if settings.negative_cache_enabled:
            is_negative = await ch.negative_get(product_id)
            if is_negative:
                _counters["neg_hit"] += 1
                layers.append("neg_cache:hit")
                return {
                    "data": None,
                    "_source": "neg_cache",
                    "_status": 404,
                    "_layers": layers,
                }
            layers.append("neg_cache:miss")

        # ── Уровень 3: Normal Cache
        cr = await ch.cache_get(product_id)
        if cr.state == "hit":
            _counters["cache_hit"] += 1
            layers.append("cache:hit")
            return {
                "data": cr.data,
                "_source": "cache",
                "_status": 200,
                "_layers": layers,
            }
        layers.append("cache:miss")

        # ── Уровень 4: DB 
        _counters["cache_miss"] += 1
        _counters["db_read"] += 1
        data = await ProductRepository.get_by_id(product_id)

        if data is not None:
            _counters["db_found"] += 1
            layers.append("db:found")
            await ch.cache_set(product_id, data)
            return {
                "data": data,
                "_source": "db",
                "_status": 200,
                "_layers": layers,
            }

        # Не найден - Bloom filter дал false positive
        _counters["db_not_found"] += 1
        layers.append("db:not_found")
        if settings.negative_cache_enabled:
            await ch.negative_set(product_id)
            layers.append("neg_cache:set")

        return {
            "data": None,
            "_source": "db_not_found",
            "_status": 404,
            "_layers": layers,
        }

    # ── CREATE 

    @staticmethod
    async def create_product(data: dict[str, Any], product_id: int | None = None) -> dict[str, Any]:
        if product_id is not None:
            result = await ProductRepository.create_with_id(product_id, data)
        else:
            result = await ProductRepository.create(data)
        # Добавить в Bloom filter + кеш, удалить negative cache
        await bloom_add(result["id"])
        await ch.cache_set(result["id"], result)
        await ch.negative_delete(result["id"])
        return {"data": result, "_source": "created", "_status": 201}

    # ── DELETE

    @staticmethod
    async def delete_product(product_id: int) -> dict[str, Any]:
        deleted = await ProductRepository.delete(product_id)
        if not deleted:
            return {"deleted": False, "_source": "not_found", "_status": 404}
        # Удалить кеш, записать negative cache
        # Из Bloom filter удалить НЕЛЬЗЯ (свойство Bloom filter!)
        await ch.cache_delete(product_id)
        if settings.negative_cache_enabled:
            await ch.negative_set(product_id)
        return {"deleted": True, "_source": "deleted", "_status": 200}

    # ── DEBUG ───────────────────────────────────────────────

    @staticmethod
    async def get_cache_debug(product_id: int) -> dict[str, Any]:
        bloom_maybe = await bloom_check(product_id)
        neg_exists = await ch.negative_get(product_id)
        cr = await ch.cache_get(product_id)
        neg_ttl = await ch.negative_get_ttl(product_id)
        cache_ttl = await ch.cache_get_ttl(product_id)

        return {
            "product_id": product_id,
            "bloom_filter": "maybe_exists" if bloom_maybe else "definitely_not",
            "negative_cache": {"exists": neg_exists, "ttl": neg_ttl},
            "normal_cache": {"state": cr.state, "ttl": cache_ttl},
            "db": await ProductRepository.get_by_id(product_id),
        }
