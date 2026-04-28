

from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Awaitable, Optional


# ── Loader — абстракция источника данных 

class DataLoader:
    """
    Базовый класс для загрузки данных из источника.
    Наследники реализуют load() и optionally load_many().
    """

    async def load(self, key: str) -> Any:
        raise NotImplementedError

    async def load_many(self, keys: list[str]) -> dict[str, Any]:
        """По умолчанию — N одиночных запросов. Можно переопределить."""
        results = {}
        for key in keys:
            results[key] = await self.load(key)
        return results


class ProductLoader(DataLoader):
    """Конкретный загрузчик продуктов из 'DB'."""

    def __init__(self):
        self._db = {
            "product:1": {"id": 1, "name": "Apple", "price": 1.50},
            "product:2": {"id": 2, "name": "Banana", "price": 0.75},
            "product:3": {"id": 3, "name": "Cherry", "price": 3.00},
        }
        self.load_count = 0

    async def load(self, key: str) -> Optional[dict]:
        self.load_count += 1
        await asyncio.sleep(0.01)   # имитация IO
        return self._db.get(key)

    async def load_many(self, keys: list[str]) -> dict[str, Any]:
        self.load_count += 1        # один batch запрос
        await asyncio.sleep(0.01)
        return {k: self._db.get(k) for k in keys}


# ── Read-Through Cache

class ReadThroughCache:
    """
    Прозрачный read-through кеш.

    Приложение взаимодействует ТОЛЬКО с кешем.
    Кеш сам знает как загрузить данные при miss.
    """

    def __init__(self, loader: DataLoader, ttl: float = 60.0):
        self.loader = loader
        self.ttl = ttl
        self._store: dict[str, tuple[Any, float]] = {}

    async def get(self, key: str) -> Any:
        """
        Получить значение.
        При miss → загрузить через loader → сохранить → вернуть.
        """
        entry = self._store.get(key)
        if entry is not None:
            value, expire_at = entry
            if time.monotonic() < expire_at:
                return value   # HIT

        # MISS — кеш сам загружает
        value = await self.loader.load(key)
        if value is not None:
            self._store[key] = (value, time.monotonic() + self.ttl)
        return value

    async def get_many(self, keys: list[str]) -> dict[str, Any]:
        """
        Batch-версия: сначала проверяем кеш для всех,
        потом один batch запрос в DB для miss'ов.
        """
        results: dict[str, Any] = {}
        miss_keys: list[str] = []
        now = time.monotonic()

        for key in keys:
            entry = self._store.get(key)
            if entry is not None:
                value, expire_at = entry
                if now < expire_at:
                    results[key] = value
                    continue
            miss_keys.append(key)

        if miss_keys:
            # Один batch запрос для всех miss
            fetched = await self.loader.load_many(miss_keys)
            for key, value in fetched.items():
                if value is not None:
                    self._store[key] = (value, now + self.ttl)
                results[key] = value

        return results

    def invalidate(self, key: str) -> None:
        """Инвалидировать ключ (при обновлении данных)."""
        self._store.pop(key, None)


# ── Декоратор паттерн 

def read_through(cache: "ReadThroughCache", key_fn: Callable = None, ttl: float = None):
    """
    Декоратор для автоматического read-through кешинга функции.

    Использование:
        @read_through(cache, key_fn=lambda pid: f"product:{pid}")
        async def get_product(product_id: int):
            return await db.fetch(product_id)
    """
    def decorator(fn: Callable):
        async def wrapper(*args, **kwargs):
            key = key_fn(*args, **kwargs) if key_fn else f"{fn.__name__}:{args}:{kwargs}"
            entry = cache._store.get(key)
            if entry is not None:
                value, expire_at = entry
                if time.monotonic() < expire_at:
                    return value
            result = await fn(*args, **kwargs)
            effective_ttl = ttl or cache.ttl
            cache._store[key] = (result, time.monotonic() + effective_ttl)
            return result
        return wrapper
    return decorator




# ── Демонстрация 

async def demo():
    loader = ProductLoader()
    cache = ReadThroughCache(loader, ttl=60.0)

    print("=== Read-Through Cache Demo ===\n")

    # 1. Cold miss
    print("1. Первый GET product:1 (cold miss):")
    p = await cache.get("product:1")
    print(f"   → {p}  (db_calls={loader.load_count})")

    # 2. HIT из кеша
    print("\n2. Повторный GET product:1 (cache hit):")
    p = await cache.get("product:1")
    print(f"   → {p}  (db_calls={loader.load_count}, не изменился!)")

    # 3. Batch get_many
    print("\n3. get_many(['product:1', 'product:2', 'product:3', 'product:99']):")
    loader.load_count = 0
    results = await cache.get_many(["product:1", "product:2", "product:3", "product:99"])
    print(f"   Результатов: {len(results)}")
    print(f"   product:1 из кеша, product:2 и product:3 — новые miss'ы")
    print(f"   db_calls={loader.load_count}  (один batch для двух miss'ов)")
    print(f"   product:99 → {results.get('product:99')}  (нет в DB)")

    print("\n--- Cache-Aside vs Read-Through ---")
    print("Cache-Aside: приложение управляет кешем вручную")
    print("Read-Through: кеш управляет сам собой, приложение не знает о DB")



if __name__ == "__main__":
    asyncio.run(demo())
