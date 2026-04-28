from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class CacheEntry:
    value: Any
    created_at: float
    fresh_ttl: float    # до этого возраста — FRESH
    error_ttl: float    # до этого возраста — можно отдавать при ошибке

    @property
    def age(self) -> float:
        return time.monotonic() - self.created_at

    @property
    def is_fresh(self) -> bool:
        return self.age < self.fresh_ttl

    @property
    def is_usable_on_error(self) -> bool:
        return self.age < self.error_ttl


@dataclass
class CacheResult:
    value: Any
    is_stale: bool      # True = данные устарели
    error: Optional[str] = None   # сообщение об ошибке если была


class ServeStaleOnErrorCache:
    """
    Кеш с fallback на устаревшие данные при ошибке источника.

    Параметры:
      fresh_ttl — секунды свежести данных (нормальный TTL)
      error_ttl — секунды хранения для fallback при ошибке
    """

    def __init__(
        self,
        fresh_ttl: float = 60.0,
        error_ttl: float = 3600.0,   # храним 1 час на случай длинного сбоя
    ):
        self.fresh_ttl = fresh_ttl
        self.error_ttl = error_ttl
        self._store: dict[str, CacheEntry] = {}

    def _make_entry(self, value: Any) -> CacheEntry:
        return CacheEntry(
            value=value,
            created_at=time.monotonic(),
            fresh_ttl=self.fresh_ttl,
            error_ttl=self.error_ttl,
        )

    async def get(self, key: str, fetch_fn) -> CacheResult:
        """
        Получить значение с защитой от ошибок источника.

        Алгоритм:
          1. Есть fresh запись → вернуть (нормальный путь)
          2. Нет fresh или нет записи → идти в DB
             a. DB успешно → обновить кеш → вернуть
             b. DB ошибка → есть stale запись → вернуть stale
             c. DB ошибка → нет записи → raise (503)
        """
        entry = self._store.get(key)

        # 1. Есть свежая запись — нормальный путь
        if entry and entry.is_fresh:
            return CacheResult(value=entry.value, is_stale=False)

        # 2. Нужно обновить — идём в источник
        try:
            value = await fetch_fn()
            self._store[key] = self._make_entry(value)
            return CacheResult(value=value, is_stale=False)

        except Exception as e:
            # 3. Ошибка источника — проверяем stale fallback
            if entry and entry.is_usable_on_error:
                return CacheResult(
                    value=entry.value,
                    is_stale=True,
                    error=f"DB error: {e}, serving stale (age={entry.age:.1f}s)",
                )
            # 4. Нечего отдавать — поднимаем ошибку
            raise RuntimeError(f"Data unavailable: DB error ({e}) and no cached fallback") from e

    def warm(self, key: str, value: Any) -> None:
        """Предзаполнить кеш (например при старте приложения)."""
        self._store[key] = self._make_entry(value)


# ── Circuit Breaker + Stale 

class CircuitBreaker:
    """
    Упрощённый Circuit Breaker.
    После N ошибок подряд → переключается в OPEN.
    В OPEN → сразу отдаём stale, не дёргаем DB.
    Через timeout → попытка восстановления (HALF-OPEN).
    """

    CLOSED = "closed"       # нормально
    OPEN = "open"           # DB считается упавшей
    HALF_OPEN = "half_open" # пробуем восстановиться

    def __init__(self, failure_threshold: int = 3, reset_timeout: float = 30.0):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self._state = self.CLOSED
        self._failures = 0
        self._opened_at: float = 0

    @property
    def state(self) -> str:
        if self._state == self.OPEN:
            if time.monotonic() - self._opened_at > self.reset_timeout:
                self._state = self.HALF_OPEN
        return self._state

    def record_success(self) -> None:
        self._failures = 0
        self._state = self.CLOSED

    def record_failure(self) -> None:
        self._failures += 1
        if self._failures >= self.failure_threshold:
            self._state = self.OPEN
            self._opened_at = time.monotonic()

    def allow_request(self) -> bool:
        return self.state in (self.CLOSED, self.HALF_OPEN)



# ── Демонстрация

async def demo():
    call_count = 0
    db_is_down = False

    async def fetch_product(product_id: int):
        nonlocal call_count
        call_count += 1
        if db_is_down:
            raise ConnectionError("DB is down!")
        await asyncio.sleep(0.01)
        return {"id": product_id, "name": "Widget", "price": 9.99}

    cache = ServeStaleOnErrorCache(fresh_ttl=0.3, error_ttl=3600.0)

    print("=== Serve-Stale-On-Error Demo ===\n")

    # 1. Нормальная работа
    result = await cache.get("product:1", lambda: fetch_product(1))
    print(f"[Нормально] stale={result.is_stale}, value={result.value['name']}")

    # 2. DB "падает"
    db_is_down = True
    await asyncio.sleep(0.35)  # ждём истечения fresh_ttl

    result = await cache.get("product:1", lambda: fetch_product(1))
    print(f"[DB упала]  stale={result.is_stale}, value={result.value['name']}")
    print(f"            error='{result.error}'")

    # 3. Полное отсутствие данных при ошибке DB
    try:
        result = await cache.get("product:999", lambda: fetch_product(999))
    except RuntimeError as e:
        print(f"[Нет данных] Exception: {e}")

    # 4. DB восстанавливается
    db_is_down = False
    result = await cache.get("product:1", lambda: fetch_product(1))
    print(f"[DB восстановлена] stale={result.is_stale}, свежее значение получено")

    print(f"\nВсего обращений к DB: {call_count}")




if __name__ == "__main__":
    asyncio.run(demo())
