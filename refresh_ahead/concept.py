from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Awaitable


class RefreshAheadCache:
    """
    Кеш с проактивным обновлением.

    Параметры:
      ttl              — время жизни записи
      refresh_threshold — доля TTL при которой запускать фоновое обновление
                          (0.2 = обновить когда осталось 20% TTL)
    """

    def __init__(self, ttl: float = 60.0, refresh_threshold: float = 0.2):
        self.ttl = ttl
        self.refresh_threshold = refresh_threshold
        self._store: dict[str, tuple[Any, float, float]] = {}
        # key → (value, expire_at, created_at)
        self._refreshing: set[str] = set()

    def _should_refresh(self, expire_at: float) -> bool:
        """Проверить нужно ли уже обновлять запись."""
        remaining = expire_at - time.monotonic()
        return remaining < self.ttl * self.refresh_threshold

    async def get(self, key: str, fetch_fn: Callable[[], Awaitable[Any]]) -> Any:
        """
        Получить значение, при необходимости запустив фоновое обновление.
        """
        entry = self._store.get(key)

        if entry is None:
            # MISS — ждём
            value = await fetch_fn()
            now = time.monotonic()
            self._store[key] = (value, now + self.ttl, now)
            return value

        value, expire_at, created_at = entry

        if time.monotonic() > expire_at:
            # Полностью устарел — ждём
            value = await fetch_fn()
            now = time.monotonic()
            self._store[key] = (value, now + self.ttl, now)
            return value

        # Проверяем порог — нужно ли запустить фоновое обновление
        if self._should_refresh(expire_at) and key not in self._refreshing:
            self._refreshing.add(key)
            asyncio.create_task(self._background_refresh(key, fetch_fn))

        return value

    async def _background_refresh(self, key: str, fetch_fn: Callable) -> None:
        """Фоновое обновление — не блокирует текущий запрос."""
        try:
            value = await fetch_fn()
            now = time.monotonic()
            self._store[key] = (value, now + self.ttl, now)
        finally:
            self._refreshing.discard(key)

    def stats(self, key: str) -> dict:
        """Отладочная информация о состоянии ключа."""
        entry = self._store.get(key)
        if entry is None:
            return {"status": "not_found"}
        value, expire_at, created_at = entry
        now = time.monotonic()
        remaining = expire_at - now
        age = now - created_at
        threshold = self.ttl * self.refresh_threshold
        return {
            "status": "fresh" if remaining > threshold else "near_expiry",
            "age_s": round(age, 2),
            "remaining_s": round(remaining, 2),
            "refresh_triggered": key in self._refreshing,
        }



# ── Демонстрация ──────────────────────────────────────────────

async def demo():
    fetch_count = 0

    async def slow_fetch():
        nonlocal fetch_count
        fetch_count += 1
        await asyncio.sleep(0.02)
        return {"data": f"version_{fetch_count}", "ts": time.time()}

    # Короткий TTL для демонстрации, порог = 50%
    cache = RefreshAheadCache(ttl=0.5, refresh_threshold=0.5)
    key = "product:1"

    print("=== Refresh-Ahead Demo ===\n")
    print("TTL=0.5s, обновление при остатке < 50% (0.25s)\n")

    # Первый запрос — MISS
    val = await cache.get(key, slow_fetch)
    print(f"t=0.0s  GET → {val['data']}  (cold miss, fetch_count={fetch_count})")

    # 0.15s — всё ещё fresh
    await asyncio.sleep(0.15)
    s = cache.stats(key)
    val = await cache.get(key, slow_fetch)
    print(f"t=0.15s GET → {val['data']}  status={s['status']} remaining={s['remaining_s']}s")

    # 0.28s — пересекаем порог 50% → фоновое обновление
    await asyncio.sleep(0.13)
    val = await cache.get(key, slow_fetch)
    s = cache.stats(key)
    print(f"t=0.28s GET → {val['data']}  status={s['status']}, refresh_triggered={s['refresh_triggered']}")

    # 0.05s — фоновое обновление должно было завершиться
    await asyncio.sleep(0.05)
    val = await cache.get(key, slow_fetch)
    print(f"t=0.33s GET → {val['data']}  (уже обновлено в фоне!)")

    # 0.6s — старый TTL истёк, но ключ уже обновлён
    await asyncio.sleep(0.27)
    val = await cache.get(key, slow_fetch)
    print(f"t=0.6s  GET → {val['data']}  (старый TTL истёк, но кеш жив!)")

    print(f"\nВсего запросов к DB: {fetch_count}")
    print("Пользователь ни разу не столкнулся с блокировкой на DB!")



if __name__ == "__main__":
    asyncio.run(demo())
