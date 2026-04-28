from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field


@dataclass
class CacheEntry:
    value: object
    fresh_until: float    # timestamp: до этого времени данные "свежие"
    stale_until: float    # timestamp: до этого — stale но допустимые


class StaleWhileRevalidateCache:
    """
    Реализация SWR паттерна в памяти.

    Параметры:
      fresh_ttl  — сколько секунд данные считаются свежими
      stale_ttl  — сколько секунд дополнительно допустимо отдавать устаревшее
    """

    def __init__(self, fresh_ttl: float = 10.0, stale_ttl: float = 60.0):
        self.fresh_ttl = fresh_ttl
        self.stale_ttl = stale_ttl
        self._store: dict[str, CacheEntry] = {}
        self._revalidating: set[str] = set()   # ключи, которые уже обновляются

    def _make_entry(self, value: object) -> CacheEntry:
        now = time.monotonic()
        return CacheEntry(
            value=value,
            fresh_until=now + self.fresh_ttl,
            stale_until=now + self.fresh_ttl + self.stale_ttl,
        )

    async def get(self, key: str, fetch_fn) -> tuple[object, str]:
        """
        Вернуть значение + статус ("fresh" | "stale" | "miss").

        Логика:
          1. Нет записи → fetch → сохранить → вернуть (MISS)
          2. Есть, fresh → вернуть (FRESH)
          3. Есть, stale → вернуть НЕМЕДЛЕННО + запустить фоновое обновление
          4. Есть, expired → fetch → сохранить → вернуть (EXPIRED→REFRESH)
        """
        now = time.monotonic()
        entry = self._store.get(key)

        # 1. Cache MISS — ждём
        if entry is None:
            value = await fetch_fn()
            self._store[key] = self._make_entry(value)
            return value, "miss"

        # 2. FRESH — отдаём без вопросов
        if now < entry.fresh_until:
            return entry.value, "fresh"

        # 3. STALE — отдаём старое, обновляем в фоне
        if now < entry.stale_until:
            if key not in self._revalidating:
                self._revalidating.add(key)
                asyncio.create_task(self._background_refresh(key, fetch_fn))
            return entry.value, "stale"

        # 4. Полностью устарело — ждём обновления
        value = await fetch_fn()
        self._store[key] = self._make_entry(value)
        return value, "expired"

    async def _background_refresh(self, key: str, fetch_fn) -> None:
        """Фоновое обновление — не блокирует текущий запрос."""
        try:
            value = await fetch_fn()
            self._store[key] = self._make_entry(value)
        finally:
            self._revalidating.discard(key)


# ── Демонстрация

async def demo():
    call_count = 0

    async def slow_db_fetch():
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.05)          # имитация DB запроса
        return {"data": f"value_v{call_count}", "fetched_at": time.time()}

    cache = StaleWhileRevalidateCache(fresh_ttl=0.2, stale_ttl=1.0)
    key = "product:42"

    print("=== Stale-While-Revalidate Demo ===\n")

    # 1. Первый запрос — MISS
    val, status = await cache.get(key, slow_db_fetch)
    print(f"[1] status={status:6s}  value={val['data']}")

    # 2. Сразу после — FRESH
    val, status = await cache.get(key, slow_db_fetch)
    print(f"[2] status={status:6s}  value={val['data']}")

    # 3. Ждём истечения fresh_ttl
    await asyncio.sleep(0.25)
    val, status = await cache.get(key, slow_db_fetch)
    print(f"[3] status={status:6s}  value={val['data']}  ← старое, но фоновое обновление запущено")

    # 4. Даём фоновому обновлению завершиться
    await asyncio.sleep(0.1)
    val, status = await cache.get(key, slow_db_fetch)
    print(f"[4] status={status:6s}  value={val['data']}  ← уже обновлено в фоне")

    print(f"\nВсего обращений к DB: {call_count}")
    print("Пользователь ни разу не ждал DB напрямую (кроме первого запроса)")



if __name__ == "__main__":
    asyncio.run(demo())
