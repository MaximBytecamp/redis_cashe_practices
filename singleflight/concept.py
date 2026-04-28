
from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Awaitable


class Flight:
    """Один "рейс" — одна выполняемая задача для ключа."""

    def __init__(self):
        self.task: asyncio.Future = asyncio.get_event_loop().create_future()
        self.waiters: int = 0


class SingleFlight:
    """
    SingleFlight группирует дублирующиеся вызовы.

    Ключевое: если N вызовов с одним ключом пришли пока задача выполняется —
    они все получат один результат, а не запустят N параллельных задач.

    Аналог: golang.org/x/sync/singleflight
    """

    def __init__(self):
        self._flights: dict[str, Flight] = {}
        self._lock = asyncio.Lock()

    async def do(self, key: str, fn: Callable[[], Awaitable[Any]]) -> tuple[Any, bool]:
        """
        Выполнить fn для key, схлопывая дубликаты.

        Возвращает (result, shared) где shared=True если результат был получен
        из уже выполняющегося запроса.
        """
        async with self._lock:
            if key in self._flights:
                # Кто-то уже выполняет этот запрос — ждём его
                flight = self._flights[key]
                flight.waiters += 1
                shared = True
            else:
                # Мы первые — создаём задачу
                flight = Flight()
                self._flights[key] = flight
                shared = False

        if shared:
            # Ждём пока первый запрос завершится
            result = await asyncio.shield(flight.task)
            return result, True

        # Мы — первый запрос, выполняем реальную работу
        try:
            result = await fn()
            flight.task.set_result(result)
            return result, False
        except Exception as e:
            flight.task.set_exception(e)
            raise
        finally:
            async with self._lock:
                del self._flights[key]


# ── SingleFlight + Cache

class SingleFlightCache:
    """
    Кеш + SingleFlight = защита от stampede.

    Поток:
      1. Проверить кеш → если есть, вернуть
      2. Если нет → через SingleFlight один запрос в DB
      3. Сохранить в кеш → вернуть всем ожидающим
    """

    def __init__(self, ttl: float = 60.0):
        self.ttl = ttl
        self._cache: dict[str, tuple[Any, float]] = {}
        self._sf = SingleFlight()

    def _get_cached(self, key: str) -> tuple[Any, bool]:
        if key in self._cache:
            value, expire_at = self._cache[key]
            if time.monotonic() < expire_at:
                return value, True
        return None, False

    async def get(self, key: str, fetch_fn: Callable[[], Awaitable[Any]]) -> Any:
        # Быстрый путь — из кеша без лока
        value, found = self._get_cached(key)
        if found:
            return value

        # Медленный путь через SingleFlight
        async def fetch_and_cache():
            val = await fetch_fn()
            self._cache[key] = (val, time.monotonic() + self.ttl)
            return val

        result, shared = await self._sf.do(key, fetch_and_cache)
        return result



# ── Демонстрация 

async def demo():
    db_calls = 0

    async def slow_db(product_id: int):
        nonlocal db_calls
        db_calls += 1
        print(f"    [DB] Запрос к базе данных для product:{product_id}")
        await asyncio.sleep(0.1)  # имитация DB
        return {"id": product_id, "name": f"Product {product_id}", "price": 99.99}

    cache = SingleFlightCache(ttl=60.0)

    print("=== SingleFlight Demo ===\n")
    print("Симулируем 10 одновременных запросов для product:42:\n")

    # 10 конкурентных запросов к одному ключу
    tasks = [
        cache.get("product:42", lambda: slow_db(42))
        for _ in range(10)
    ]
    results = await asyncio.gather(*tasks)

    print(f"\nРезультатов получено: {len(results)}")
    print(f"Запросов к DB: {db_calls}  (ожидалось: 1!)")
    assert db_calls == 1, "SingleFlight должен был схлопнуть все запросы в один!"

    print("\nВторой раунд (кеш тёплый):")
    db_calls = 0
    tasks = [cache.get("product:42", lambda: slow_db(42)) for _ in range(10)]
    results = await asyncio.gather(*tasks)
    print(f"Запросов к DB: {db_calls}  (ожидалось: 0 — из кеша)")

    print("\n✓ SingleFlight работает корректно")





if __name__ == "__main__":
    asyncio.run(demo())
