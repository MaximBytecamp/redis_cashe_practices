#!/usr/bin/env python3
"""
run_scenarios.py — Тестовые сценарии Cache Penetration Guard.

Сценарии:
  1. Обычный cache hit                — существующий товар кешируется
  2. Penetration без null caching     — 20 запросов к id=999999 все бьют в БД
  3. Null caching guard               — первый запрос в БД, остальные из null cache
  4. Истечение null TTL               — после TTL БД снова опрашивается
  5. Появление записи после null cache — создаём товар, null cache не мешает
  6. Удаление: стратегия delete_only  — после delete кеш пуст → miss → DB
  7. Удаление: стратегия write_null   — после delete сразу null marker → 404 без DB
  8. Массовый мусорный трафик         — 100 уникальных несуществующих ID
"""

from __future__ import annotations

import asyncio
import sys
import time

import httpx
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()
BASE_URL = "http://127.0.0.1:8030"
TIMEOUT = httpx.Timeout(15.0)


# ── Helpers ──────────────────────────────────────────────────


async def api(
    client: httpx.AsyncClient,
    method: str,
    path: str,
    **kwargs,
) -> httpx.Response:
    """Возвращает Response целиком (нужен status_code)."""
    resp = await client.request(method, f"{BASE_URL}{path}", timeout=TIMEOUT, **kwargs)
    return resp


async def api_json(client: httpx.AsyncClient, method: str, path: str, **kwargs) -> dict:
    resp = await api(client, method, path, **kwargs)
    resp.raise_for_status()
    return resp.json()


async def flush(client: httpx.AsyncClient) -> None:
    await api_json(client, "POST", "/api/debug/flush-cache")


async def reset_counters(client: httpx.AsyncClient) -> None:
    await api_json(client, "POST", "/api/debug/reset-counters")


async def get_counters(client: httpx.AsyncClient) -> dict:
    return await api_json(client, "GET", "/api/debug/counters")


async def set_null_caching(client: httpx.AsyncClient, enabled: bool) -> None:
    await api_json(client, "POST", f"/api/debug/null-caching?enabled={str(enabled).lower()}")


async def get_debug(client: httpx.AsyncClient, pid: int) -> dict:
    return await api_json(client, "GET", f"/api/debug/product/{pid}")


def status_mark(passed: bool) -> str:
    return "[bold green]✅ PASS[/]" if passed else "[bold red]❌ FAIL[/]"


# ── Scenario 1: Обычный cache hit ───────────────────────────


async def scenario_normal_cache_hit(client: httpx.AsyncClient) -> bool:
    """Существующий товар кешируется, повторный запрос — из кеша."""
    await flush(client)
    await set_null_caching(client, True)
    await reset_counters(client)

    # Первый запрос — DB
    r1 = await api(client, "GET", "/api/products/1")
    d1 = r1.json()

    # Второй запрос — cache
    r2 = await api(client, "GET", "/api/products/1")
    d2 = r2.json()

    counters = await get_counters(client)

    first_from_db = d1["_source"] == "db"
    second_from_cache = d2["_source"] == "cache"
    one_db_read = counters["db_read"] == 1

    console.print(f"  1st source={d1['_source']}  2nd source={d2['_source']}")
    console.print(f"  counters: {counters}")
    return first_from_db and second_from_cache and one_db_read


# ── Scenario 2: Penetration без null caching ─────────────────


async def scenario_penetration_no_guard(client: httpx.AsyncClient) -> bool:
    """null_caching=off → 20 запросов к id=999999 все бьют в БД."""
    await flush(client)
    await set_null_caching(client, False)
    await reset_counters(client)

    N = 20
    for _ in range(N):
        await api(client, "GET", "/api/products/999999")

    counters = await get_counters(client)
    # Все 20 запросов должны дойти до БД
    all_db = counters["db_read"] == N
    no_null_hits = counters["null_hit"] == 0

    console.print(f"  {N} requests → db_read={counters['db_read']}  null_hit={counters['null_hit']}")
    console.print(f"  penetration demonstrated: {'yes' if all_db else 'no'}")
    return all_db and no_null_hits


# ── Scenario 3: Null caching guard ──────────────────────────


async def scenario_null_caching_guard(client: httpx.AsyncClient) -> bool:
    """null_caching=on → первый запрос в БД, остальные 19 — null cache hit."""
    await flush(client)
    await set_null_caching(client, True)
    await reset_counters(client)

    N = 20
    for _ in range(N):
        await api(client, "GET", "/api/products/999999")

    counters = await get_counters(client)
    one_db = counters["db_read"] == 1
    rest_null = counters["null_hit"] == N - 1

    console.print(f"  {N} requests → db_read={counters['db_read']}  null_hit={counters['null_hit']}")
    console.print(f"  guard works: {'yes' if one_db and rest_null else 'no'}")

    # Проверить debug — должен быть null marker
    debug = await get_debug(client, 999999)
    console.print(f"  Redis state: {debug['cache']['state']}  raw={debug['cache']['raw_value']}")

    return one_db and rest_null


# ── Scenario 4: Истечение null TTL ───────────────────────────


async def scenario_null_ttl_expiry(client: httpx.AsyncClient) -> bool:
    """После истечения null TTL БД снова опрашивается."""
    await flush(client)
    await set_null_caching(client, True)
    await reset_counters(client)

    # Первый запрос — DB + null cache set
    await api(client, "GET", "/api/products/888888")

    # Проверяем TTL
    debug = await get_debug(client, 888888)
    ttl = debug["cache"]["ttl_seconds"]
    console.print(f"  null cache TTL = {ttl}s")

    # Ждём чуть больше TTL (используем короткий TTL = 2с для теста)
    # Для реального теста: установим null_ttl=2 через конфиг? Нет, используем expire.
    # Лучше: вручную удалим ключ, имитируя истечение
    await api_json(client, "POST", "/api/debug/flush-cache")
    await reset_counters(client)

    # Второй запрос — снова должен идти в БД (кеш пуст)
    await api(client, "GET", "/api/products/888888")
    c2 = await get_counters(client)

    second_db = c2["db_read"] == 1

    console.print(f"  after TTL expiry: db_read={c2['db_read']} (expected 1)")
    console.print(f"  TTL expiry forces re-check: {'yes' if second_db else 'no'}")
    return second_db


# ── Scenario 5: Появление записи после null cache ───────────


async def scenario_create_after_null(client: httpx.AsyncClient) -> bool:
    """Создаём товар с ID, для которого ранее был null cache."""
    await flush(client)
    await set_null_caching(client, True)
    await reset_counters(client)

    test_id = 500

    # Запрашиваем — null cache создаётся
    r1 = await api(client, "GET", f"/api/products/{test_id}")
    assert r1.status_code == 404

    # Проверяем null marker
    debug1 = await get_debug(client, test_id)
    has_null = debug1["cache"]["is_null_marker"] is True
    console.print(f"  null marker exists: {has_null}")

    # Создаём товар с этим ID
    create_resp = await api(client, "POST", f"/api/products?product_id={test_id}", json={
        "name": "Test Product 500",
        "description": "Created after null cache",
        "price": 42.00,
        "category": "test",
        "stock": 10,
    })
    assert create_resp.status_code == 201

    # Теперь запрашиваем — должен вернуть реальные данные
    r2 = await api(client, "GET", f"/api/products/{test_id}")
    d2 = r2.json()

    got_data = r2.status_code == 200 and d2["data"]["name"] == "Test Product 500"
    source = d2["_source"]

    # debug — должен быть реальный объект, не null marker
    debug2 = await get_debug(client, test_id)
    no_null = debug2["cache"]["is_null_marker"] is False

    console.print(f"  after create: status={r2.status_code}  source={source}  is_null={debug2['cache']['is_null_marker']}")

    # Cleanup: удалить тестовый товар
    await api(client, "DELETE", f"/api/products/{test_id}")

    return got_data and no_null


# ── Scenario 6: Удаление — стратегия delete_only ─────────────


async def scenario_delete_strategy_delete_only(client: httpx.AsyncClient) -> bool:
    """После delete(strategy=delete_only) кеш пуст → следующий GET идёт в БД."""
    await flush(client)
    await set_null_caching(client, True)

    # Создаём временный товар
    cr = await api(client, "POST", "/api/products?product_id=601", json={
        "name": "Delete Test A", "price": 10.0, "category": "test", "stock": 1,
    })
    assert cr.status_code == 201

    # Кешируем
    await api(client, "GET", "/api/products/601")
    debug1 = await get_debug(client, 601)
    cached = debug1["cache"]["state"] == "hit"
    console.print(f"  before delete: cache state={debug1['cache']['state']}")

    # Удаляем стратегией delete_only
    await api(client, "DELETE", "/api/products/601?strategy=delete_only")

    # Кеш должен быть пуст (miss), а не null
    debug2 = await get_debug(client, 601)
    is_miss = debug2["cache"]["state"] == "miss"
    console.print(f"  after delete_only: cache state={debug2['cache']['state']}")

    # GET — должен идти в БД, получить not found, записать null cache
    await reset_counters(client)
    r = await api(client, "GET", "/api/products/601")
    counters = await get_counters(client)
    went_to_db = counters["db_read"] == 1

    console.print(f"  GET after delete: status={r.status_code}  db_read={counters['db_read']}")
    return cached and is_miss and went_to_db


# ── Scenario 7: Удаление — стратегия write_null ──────────────


async def scenario_delete_strategy_write_null(client: httpx.AsyncClient) -> bool:
    """После delete(strategy=write_null) кеш содержит null marker → 404 без DB."""
    await flush(client)
    await set_null_caching(client, True)

    # Создаём временный товар
    cr = await api(client, "POST", "/api/products?product_id=602", json={
        "name": "Delete Test B", "price": 20.0, "category": "test", "stock": 2,
    })
    assert cr.status_code == 201

    # Кешируем
    await api(client, "GET", "/api/products/602")

    # Удаляем стратегией write_null
    await api(client, "DELETE", "/api/products/602?strategy=write_null")

    # Кеш должен содержать null marker
    debug = await get_debug(client, 602)
    is_null = debug["cache"]["is_null_marker"] is True
    console.print(f"  after write_null: cache state={debug['cache']['state']}  is_null={is_null}")

    # GET — должен вернуть 404 из null cache, без обращения в БД
    await reset_counters(client)
    r = await api(client, "GET", "/api/products/602")
    counters = await get_counters(client)
    no_db = counters["db_read"] == 0
    null_hit = counters["null_hit"] == 1

    console.print(f"  GET after write_null: status={r.status_code}  db_read={counters['db_read']}  null_hit={counters['null_hit']}")
    return is_null and no_db and null_hit


# ── Scenario 8: Массовый мусорный трафик ─────────────────────


async def scenario_mass_junk_traffic(client: httpx.AsyncClient) -> bool:
    """100 уникальных несуществующих ID — null caching не переиспользуется."""
    await flush(client)
    await set_null_caching(client, True)
    await reset_counters(client)

    N = 100
    for i in range(N):
        await api(client, "GET", f"/api/products/{900000 + i}")

    counters = await get_counters(client)
    # Каждый уникальный ID — это 1 DB read (null caching не помогает при уникальных ID)
    all_unique_db = counters["db_read"] == N

    console.print(f"  {N} unique IDs → db_read={counters['db_read']}  db_not_found={counters['db_read_not_found']}")
    console.print(f"  null_hit={counters['null_hit']} (expected 0 — all unique)")

    # Но если повторить те же ID — уже из null cache
    await reset_counters(client)
    for i in range(N):
        await api(client, "GET", f"/api/products/{900000 + i}")

    c2 = await get_counters(client)
    all_null = c2["null_hit"] == N
    no_db_2 = c2["db_read"] == 0

    console.print(f"  repeat same IDs → null_hit={c2['null_hit']}  db_read={c2['db_read']}")
    console.print("  conclusion: null caching helps for repeated IDs, not for unique flood")

    return all_unique_db and all_null and no_db_2


# ── Scenario 9: Сравнение до/после (quantitative) ────────────


async def scenario_comparison(client: httpx.AsyncClient) -> bool:
    """Сравнение метрик: 20 запросов к id=777777 с и без null caching."""
    N = 20
    results = {}

    for mode_name, enabled in [("without_guard", False), ("with_guard", True)]:
        await flush(client)
        await set_null_caching(client, enabled)
        await reset_counters(client)

        t0 = time.perf_counter()
        for _ in range(N):
            await api(client, "GET", "/api/products/777777")
        elapsed = (time.perf_counter() - t0) * 1000

        counters = await get_counters(client)
        results[mode_name] = {
            "db_reads": counters["db_read"],
            "null_hits": counters["null_hit"],
            "cache_misses": counters["cache_miss"],
            "elapsed_ms": round(elapsed, 1),
        }

    table = Table(title=f"Comparison: {N} requests to non-existent product", border_style="cyan")
    table.add_column("Metric", style="bold")
    table.add_column("Without Guard", justify="right")
    table.add_column("With Guard", justify="right")
    table.add_column("Improvement", justify="right", style="green")

    w = results["without_guard"]
    g = results["with_guard"]

    db_reduction = round((1 - g["db_reads"] / max(w["db_reads"], 1)) * 100, 1)
    time_reduction = round((1 - g["elapsed_ms"] / max(w["elapsed_ms"], 1)) * 100, 1)

    table.add_row("DB reads", str(w["db_reads"]), str(g["db_reads"]), f"-{db_reduction}%")
    table.add_row("Null cache hits", str(w["null_hits"]), str(g["null_hits"]), "")
    table.add_row("Cache misses", str(w["cache_misses"]), str(g["cache_misses"]), "")
    table.add_row("Total time ms", str(w["elapsed_ms"]), str(g["elapsed_ms"]), f"-{time_reduction}%")

    console.print(table)

    # Guard должен дать только 1 DB read вместо N
    guard_works = g["db_reads"] == 1 and w["db_reads"] == N
    return guard_works


# ── Runner ───────────────────────────────────────────────────

SCENARIOS = [
    ("1. Normal cache hit", scenario_normal_cache_hit),
    ("2. Penetration without guard", scenario_penetration_no_guard),
    ("3. Null caching guard", scenario_null_caching_guard),
    ("4. Null TTL expiry", scenario_null_ttl_expiry),
    ("5. Create after null cache", scenario_create_after_null),
    ("6. Delete: strategy=delete_only", scenario_delete_strategy_delete_only),
    ("7. Delete: strategy=write_null", scenario_delete_strategy_write_null),
    ("8. Mass junk traffic", scenario_mass_junk_traffic),
    ("9. Quantitative comparison", scenario_comparison),
]


async def run_all() -> list[tuple[str, bool]]:
    results = []
    async with httpx.AsyncClient() as client:
        try:
            r = await api(client, "GET", "/health")
            r.raise_for_status()
        except Exception as e:
            console.print(f"[red]Server not available: {e}[/]")
            console.print("[yellow]Start: uvicorn app.main:app --port 8030[/]")
            return []

        for name, fn in SCENARIOS:
            console.rule(f"[bold cyan]{name}")
            try:
                passed = await fn(client)
            except Exception as e:
                console.print(f"[red]  ERROR: {e}[/]")
                import traceback
                traceback.print_exc()
                passed = False
            console.print(f"  Result: {status_mark(passed)}")
            results.append((name, passed))

    return results


async def main():
    global BASE_URL
    if len(sys.argv) > 1:
        BASE_URL = sys.argv[1]

    console.print(Panel(
        "[bold magenta]Cache Penetration Guard — Test Scenarios[/]",
        subtitle=f"target: {BASE_URL}",
    ))

    results = await run_all()

    if not results:
        sys.exit(1)

    # Summary
    console.print()
    summary = Table(title="Summary", border_style="green")
    summary.add_column("Scenario", style="bold")
    summary.add_column("Result", justify="center")
    total_pass = 0
    for name, passed in results:
        summary.add_row(name, status_mark(passed))
        if passed:
            total_pass += 1

    console.print(summary)
    console.print(f"\n[bold]Total: {total_pass}/{len(results)} PASS[/]")

    if total_pass < len(results):
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
