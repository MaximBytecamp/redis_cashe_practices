from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Awaitable


class RequestCoalescer:
    """
    Объединяет запросы за временно́е окно в один batch.

    batch_fn: async fn(keys: list) -> dict[key, value]
    window_ms: время ожидания накопления запросов (миллисекунды)
    """

    def __init__(
        self,
        batch_fn: Callable[[list], Awaitable[dict]],
        window_ms: float = 5.0,
    ):
        self.batch_fn = batch_fn
        self.window_s = window_ms / 1000

        self._pending: dict[str, list[asyncio.Future]] = {}
        self._lock = asyncio.Lock()
        self._window_task: asyncio.Task | None = None

    async def get(self, key: str) -> Any:
        """
        Запросить значение для ключа.
        Запрос будет объединён с другими запросами в текущем окне.
        """
        future: asyncio.Future = asyncio.get_event_loop().create_future()

        async with self._lock:
            if key not in self._pending:
                self._pending[key] = []
            self._pending[key].append(future)

            # Запустить таймер окна если ещё не запущен
            if self._window_task is None or self._window_task.done():
                self._window_task = asyncio.create_task(self._flush_window())

        return await future

    async def _flush_window(self) -> None:
        """Подождать окно, потом выполнить batch запрос."""
        await asyncio.sleep(self.window_s)

        async with self._lock:
            if not self._pending:
                return
            batch = dict(self._pending)
            self._pending.clear()

        keys = list(batch.keys())
        try:
            results = await self.batch_fn(keys)
            # Разослать результаты всем ожидающим
            for key, futures in batch.items():
                value = results.get(key)
                for fut in futures:
                    if not fut.done():
                        fut.set_result(value)
        except Exception as e:
            for futures in batch.values():
                for fut in futures:
                    if not fut.done():
                        fut.set_exception(e)


# ── Coalescer с кешем 

class CoalescingCache:
    """
    Кеш + Coalescing: сначала проверяем кеш, потом объединяем miss'ы.
    """

    def __init__(
        self,
        batch_fn: Callable[[list], Awaitable[dict]],
        ttl: float = 60.0,
        window_ms: float = 5.0,
    ):
        self.ttl = ttl
        self._store: dict[str, tuple[Any, float]] = {}
        self._coalescer = RequestCoalescer(
            batch_fn=self._fetch_missing,
            window_ms=window_ms,
        )
        self._batch_fn = batch_fn

    async def _fetch_missing(self, keys: list) -> dict:
        return await self._batch_fn(keys)

    async def get(self, key: str) -> Any:
        # Быстрый путь — из кеша
        entry = self._store.get(key)
        if entry:
            value, expire_at = entry
            if time.monotonic() < expire_at:
                return value

        # Медленный путь — через coalescer
        value = await self._coalescer.get(key)
        if value is not None:
            self._store[key] = (value, time.monotonic() + self.ttl)
        return value





# ── Демонстрация

async def demo():
    db_calls: list[list] = []

    async def batch_db_fetch(keys: list) -> dict:
        db_calls.append(keys)
        print(f"    [DB] Batch запрос: SELECT WHERE id IN {keys}")
        await asyncio.sleep(0.02)
        return {key: {"id": key, "name": f"Product {key}"} for key in keys}

    cache = CoalescingCache(batch_fn=batch_db_fetch, ttl=60.0, window_ms=10)

    print("=== Request Coalescing Demo ===\n")
    print("Запускаем 10 параллельных запросов к 5 разным ключам:\n")

    # 10 параллельных запросов к 5 ключам
    tasks = []
    for i in range(10):
        key = f"product:{(i % 5) + 1}"   # ключи 1..5, каждый дважды
        tasks.append(cache.get(key))

    results = await asyncio.gather(*tasks)

    print(f"\nПолучено результатов: {len(results)}")
    print(f"Количество batch-запросов к DB: {len(db_calls)}")
    print(f"Ключей запрошено в одном batch: {[len(b) for b in db_calls]}")

    print("\nВторой раунд (кеш тёплый):")
    db_calls.clear()
    results = await asyncio.gather(*[cache.get(f"product:{i+1}") for i in range(5)])
    print(f"Запросов к DB: {len(db_calls)}  (из кеша, без DB запросов)")


if __name__ == "__main__":
    asyncio.run(demo())
