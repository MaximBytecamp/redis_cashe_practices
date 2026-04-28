from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from typing import Any


class DirtyBuffer:
    """
    Буфер грязных записей — хранит изменения ещё не сохранённые в DB.
    Использует OrderedDict для сохранения порядка записей.
    """

    def __init__(self):
        self._buffer: OrderedDict[str, Any] = OrderedDict()
        self._timestamps: dict[str, float] = {}

    def mark_dirty(self, key: str, value: Any) -> None:
        """Пометить ключ как изменённый."""
        self._buffer[key] = value
        self._timestamps[key] = time.monotonic()

    def get_dirty_keys(self) -> list[str]:
        return list(self._buffer.keys())

    def get_value(self, key: str) -> Any:
        return self._buffer.get(key)

    def flush_keys(self, keys: list[str]) -> dict[str, Any]:
        """Извлечь ключи из буфера (удалить из dirty)."""
        flushed = {}
        for key in keys:
            if key in self._buffer:
                flushed[key] = self._buffer.pop(key)
                self._timestamps.pop(key, None)
        return flushed

    def age_of(self, key: str) -> float:
        """Сколько секунд запись ждёт в буфере."""
        ts = self._timestamps.get(key, time.monotonic())
        return time.monotonic() - ts

    def __len__(self) -> int:
        return len(self._buffer)


class WriteBehindCache:
    """
    Write-Behind кеш с фоновым worker'ом.

    Параметры:
      flush_interval — секунды между flush в DB
      max_dirty      — если буфер >= max_dirty → немедленный flush
      ttl            — TTL записей в кеше
    """

    def __init__(
        self,
        db_writer,                  # async fn(data: dict) -> None
        flush_interval: float = 1.0,
        max_dirty: int = 100,
        ttl: float = 300.0,
    ):
        self.db_writer = db_writer
        self.flush_interval = flush_interval
        self.max_dirty = max_dirty
        self.ttl = ttl

        self._store: dict[str, tuple[Any, float]] = {}
        self._dirty = DirtyBuffer()
        self._flush_count = 0
        self._worker_task: asyncio.Task | None = None

    def start(self) -> None:
        """Запустить фоновый worker."""
        self._worker_task = asyncio.create_task(self._flush_worker())

    async def stop(self) -> None:
        """Остановить worker и сделать финальный flush."""
        if self._worker_task:
            self._worker_task.cancel()
        await self.flush()   # финальный сброс в DB

    async def get(self, key: str) -> Any:
        """Читаем из кеша (в т.ч. из dirty buffer)."""
        entry = self._store.get(key)
        if entry is not None:
            value, expire_at = entry
            if time.monotonic() < expire_at:
                return value
        return None

    async def set(self, key: str, value: Any) -> None:
        """
        Записать в кеш МГНОВЕННО.
        Асинхронно — в DB через background worker.
        """
        self._store[key] = (value, time.monotonic() + self.ttl)
        self._dirty.mark_dirty(key, value)

        # Если буфер переполнен → немедленный flush
        if len(self._dirty) >= self.max_dirty:
            await self.flush()

    async def flush(self) -> int:
        """Сбросить все dirty записи в DB."""
        dirty_keys = self._dirty.get_dirty_keys()
        if not dirty_keys:
            return 0

        data = self._dirty.flush_keys(dirty_keys)
        await self.db_writer(data)
        self._flush_count += 1
        return len(data)

    async def _flush_worker(self) -> None:
        """Фоновый worker: периодически делает flush."""
        while True:
            await asyncio.sleep(self.flush_interval)
            try:
                count = await self.flush()
                if count:
                    pass  # можно логировать
            except Exception:
                pass  # не падаем при ошибке DB




# ── Демонстрация

async def demo():
    written_to_db: list[dict] = []

    async def db_writer(data: dict) -> None:
        written_to_db.append(data)
        print(f"    [DB] Batch write: {list(data.keys())}")
        await asyncio.sleep(0.01)  # имитация DB write latency

    cache = WriteBehindCache(
        db_writer=db_writer,
        flush_interval=0.2,
        max_dirty=5,
        ttl=60.0,
    )
    cache.start()

    print("=== Write-Behind Cache Demo ===\n")
    print("5 записей — все мгновенно в кеш, в DB — асинхронно:\n")

    t0 = time.monotonic()
    for i in range(1, 6):
        await cache.set(f"product:{i}", {"id": i, "views": i * 10})
        print(f"  SET product:{i} → OK (elapsed: {(time.monotonic()-t0)*1000:.1f}ms)")

    print(f"\nВсего dirty записей в буфере: {len(cache._dirty)}")
    print("В DB ещё НЕ записано (асинхронно).\n")

    # Ждём автоматического flush
    await asyncio.sleep(0.3)

    print(f"\nПосле flush_interval=0.2s:")
    print(f"  Flush вызовов: {cache._flush_count}")
    print(f"  Dirty записей в буфере: {len(cache._dirty)}")

    # Читаем из кеша
    p = await cache.get("product:3")
    print(f"\nGET product:3 из кеша: {p}")

    await cache.stop()
    print("\nWorker остановлен, финальный flush выполнен.")



if __name__ == "__main__":
    asyncio.run(demo())
