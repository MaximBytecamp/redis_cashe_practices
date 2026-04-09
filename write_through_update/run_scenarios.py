#!/usr/bin/env python3
"""
run_scenarios.py — Тестовые сценарии Write-Through After Update.

Сценарии:
  1. Stale Cache (sync_mode=none)  — показываем проблему
  2. Write-Through Card             — карточка мгновенно актуальна
  3. Invalidate vs Write-Through    — сравнение двух стратегий
  4. Hybrid Strategy                — карточка WT + списки invalidate
  5. Category Change                — обновление при смене категории
  6. Batch Price Update             — массовое обновление + sync
  7. Load Test + Comparison         — 100 update+read, метрики по режимам
"""

from __future__ import annotations

import asyncio
import statistics
import sys
import time

import httpx
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()
BASE_URL = "http://127.0.0.1:8020"
TIMEOUT = httpx.Timeout(15.0)

# ── Helpers ──────────────────────────────────────────────────


async def api(
    client: httpx.AsyncClient,
    method: str,
    path: str,
    **kwargs,
) -> dict:
    resp = await client.request(method, f"{BASE_URL}{path}", timeout=TIMEOUT, **kwargs)
    resp.raise_for_status()
    return resp.json()


async def set_mode(client: httpx.AsyncClient, mode: str) -> None:
    await api(client, "POST", f"/api/debug/sync-mode?mode={mode}")


async def flush(client: httpx.AsyncClient) -> None:
    await api(client, "POST", "/api/debug/flush-cache")


def status_mark(passed: bool) -> str:
    return "[bold green]✅ PASS[/]" if passed else "[bold red]❌ FAIL[/]"


# ── Scenario 1: Stale Cache ─────────────────────────────────


async def scenario_stale_cache(client: httpx.AsyncClient) -> bool:
    """sync_mode=none → кеш не обновляется → stale-данные."""
    await flush(client)
    await set_mode(client, "none")

    # Загружаем в кеш
    p = await api(client, "GET", "/api/products/1")
    old_price = p["price"]

    # Обновляем цену
    new_price = old_price + 111.11
    await api(client, "PUT", "/api/products/1", json={"price": new_price})

    # Читаем из кеша — должна быть СТАРАЯ цена
    cached = await api(client, "GET", "/api/products/1")
    is_stale = cached["price"] == old_price and cached["_source"] == "cache"

    # Восстанавливаем
    await api(client, "PUT", "/api/products/1", json={"price": old_price})

    console.print(f"  old_price={old_price}  new_price={new_price}")
    console.print(f"  cached_price={cached['price']}  source={cached['_source']}")
    console.print(f"  stale detected: {is_stale}")
    return is_stale


# ── Scenario 2: Write-Through Card ──────────────────────────


async def scenario_write_through_card(client: httpx.AsyncClient) -> bool:
    """sync_mode=write_through → карточка в кеше сразу актуальна."""
    await flush(client)
    await set_mode(client, "write_through")

    # Грузим в кеш
    p = await api(client, "GET", "/api/products/2")
    old_price = p["price"]

    new_price = old_price + 222.22
    result = await api(client, "PUT", "/api/products/2", json={"price": new_price})
    sync = result.get("_sync", {})

    # Читаем — должна быть НОВАЯ цена из кеша
    cached = await api(client, "GET", "/api/products/2")
    is_fresh = (
        abs(cached["price"] - new_price) < 0.01
        and cached["_source"] == "cache"
    )

    # Восстанавливаем
    await api(client, "PUT", "/api/products/2", json={"price": old_price})

    console.print(f"  write-through keys: {sync.get('wt_keys', [])}")
    console.print(f"  cached_price={cached['price']}  expected={new_price}  source={cached['_source']}")
    return is_fresh


# ── Scenario 3: Invalidate vs Write-Through ──────────────────


async def scenario_invalidate_vs_wt(client: httpx.AsyncClient) -> bool:
    """Сравнение: invalidate удаляет ключ → cache miss,
    write-through обновляет → cache hit."""
    results = {}

    for mode in ("invalidate", "write_through"):
        await flush(client)
        await set_mode(client, mode)

        # Прогреваем кеш
        await api(client, "GET", "/api/products/3")

        # Обновляем
        await api(client, "PUT", "/api/products/3", json={"price": 999.99})

        # Читаем
        after = await api(client, "GET", "/api/products/3")
        results[mode] = after["_source"]

        # Восстанавливаем
        await api(client, "PUT", "/api/products/3", json={"price": 1899.00})

    # invalidate → cache miss → _source=db;  write_through → cache hit → _source=cache
    inv_miss = results["invalidate"] == "db"
    wt_hit = results["write_through"] == "cache"

    console.print(f"  invalidate → source={results['invalidate']}  (expected db)")
    console.print(f"  write_through → source={results['write_through']}  (expected cache)")
    return inv_miss and wt_hit


# ── Scenario 4: Hybrid Strategy ─────────────────────────────


async def scenario_hybrid(client: httpx.AsyncClient) -> bool:
    """hybrid: карточка — cache hit, список — cache miss (invalidated)."""
    await flush(client)
    await set_mode(client, "hybrid")

    # Прогреваем
    await api(client, "GET", "/api/products/4")
    await api(client, "GET", "/api/products")

    # Обновляем
    result = await api(client, "PUT", "/api/products/4", json={"price": 888.88})
    sync = result.get("_sync", {})

    # Карточка — должна быть из кеша (write-through)
    card = await api(client, "GET", "/api/products/4")
    card_hit = card["_source"] == "cache"

    # Список — должен быть из БД (invalidated)
    lst = await api(client, "GET", "/api/products")
    list_miss = lst["_source"] == "db"

    # Восстанавливаем
    await api(client, "PUT", "/api/products/4", json={"price": 1199.00})

    console.print(f"  sync: wt_keys={sync.get('wt_keys', [])}  inv_keys={sync.get('invalidated_keys', [])}")
    console.print(f"  card source={card['_source']} (expect cache)  list source={lst['_source']} (expect db)")
    return card_hit and list_miss


# ── Scenario 5: Category Change ─────────────────────────────


async def scenario_category_change(client: httpx.AsyncClient) -> bool:
    """При смене категории инвалидируются обе категории."""
    await flush(client)
    await set_mode(client, "hybrid")

    # Прогреваем кеш обеих категорий
    await api(client, "GET", "/api/products/category/phones")
    await api(client, "GET", "/api/products/category/tablets")

    # Перемещаем товар 4 (phones → tablets)
    result = await api(client, "PUT", "/api/products/4", json={"category": "tablets"})
    sync = result.get("_sync", {})

    # Оба списка должны быть invalidated
    phones = await api(client, "GET", "/api/products/category/phones")
    tablets = await api(client, "GET", "/api/products/category/tablets")
    both_miss = phones["_source"] == "db" and tablets["_source"] == "db"

    # Восстанавливаем
    await api(client, "PUT", "/api/products/4", json={"category": "phones"})

    console.print(f"  invalidated: {sync.get('invalidated_keys', [])}")
    console.print(f"  phones source={phones['_source']}  tablets source={tablets['_source']}")
    return both_miss


# ── Scenario 6: Batch Price Update ──────────────────────────


async def scenario_batch_update(client: httpx.AsyncClient) -> bool:
    """Массовое обновление цен — карточки обновлены, списки инвалидированы."""
    await flush(client)
    await set_mode(client, "hybrid")

    # Прогреваем
    monitors = await api(client, "GET", "/api/products/category/monitors")
    for p in monitors["products"]:
        await api(client, "GET", f"/api/products/{p['id']}")

    # Скидка 10% на мониторы
    result = await api(
        client, "POST",
        "/api/products/batch-price?category=monitors&multiplier=0.9",
    )

    # Проверяем карточку первого монитора — из кеша и с новой ценой
    first_id = result["products"][0]["id"]
    expected_price = result["products"][0]["price"]
    card = await api(client, "GET", f"/api/products/{first_id}")
    card_ok = card["_source"] == "cache" and abs(card["price"] - expected_price) < 0.01

    # Список — invalidated
    cat = await api(client, "GET", "/api/products/category/monitors")
    list_miss = cat["_source"] == "db"

    # Откат
    await api(
        client, "POST",
        f"/api/products/batch-price?category=monitors&multiplier={round(1/0.9, 6)}",
    )

    console.print(f"  updated {result['updated_count']} monitors, sync_ms={result.get('_sync_ms')}")
    console.print(f"  card source={card['_source']} price_ok={card_ok}  list source={cat['_source']}")
    return card_ok and list_miss


# ── Scenario 7: Load Test + Mode Comparison ──────────────────


async def scenario_load_comparison(client: httpx.AsyncClient) -> bool:
    """100 update+read для каждого режима, сравнение cache hit/miss и latency."""
    modes = ["write_through", "invalidate", "hybrid", "none"]
    N = 50  # iterations per mode
    table = Table(title="Load Test Comparison (50 iterations)", border_style="cyan")
    table.add_column("Mode", style="bold")
    table.add_column("Avg Update ms", justify="right")
    table.add_column("Avg Read ms", justify="right")
    table.add_column("Cache Hits %", justify="right")
    table.add_column("Stale Reads", justify="right")

    product_id = 5
    original = await api(client, "GET", f"/api/products/{product_id}")
    original_price = original["price"]

    all_ok = True
    for mode in modes:
        await flush(client)
        await set_mode(client, mode)

        # Prime cache
        await api(client, "GET", f"/api/products/{product_id}")

        update_times = []
        read_times = []
        cache_hits = 0
        stale_reads = 0

        for i in range(N):
            new_p = original_price + i * 0.01

            t0 = time.perf_counter()
            await api(client, "PUT", f"/api/products/{product_id}", json={"price": new_p})
            update_times.append((time.perf_counter() - t0) * 1000)

            t1 = time.perf_counter()
            r = await api(client, "GET", f"/api/products/{product_id}")
            read_times.append((time.perf_counter() - t1) * 1000)

            if r["_source"] == "cache":
                cache_hits += 1
            if abs(r["price"] - new_p) > 0.01:
                stale_reads += 1

        hit_pct = round(cache_hits / N * 100, 1)
        table.add_row(
            mode,
            f"{statistics.mean(update_times):.1f}",
            f"{statistics.mean(read_times):.1f}",
            f"{hit_pct}%",
            str(stale_reads),
        )

        # write_through и hybrid должны иметь 0 stale reads
        if mode in ("write_through", "hybrid") and stale_reads > 0:
            all_ok = False

    # Восстанавливаем
    await set_mode(client, "hybrid")
    await api(client, "PUT", f"/api/products/{product_id}", json={"price": original_price})

    console.print(table)
    return all_ok


# ── Runner ───────────────────────────────────────────────────

SCENARIOS = [
    ("1. Stale Cache (mode=none)", scenario_stale_cache),
    ("2. Write-Through Card", scenario_write_through_card),
    ("3. Invalidate vs Write-Through", scenario_invalidate_vs_wt),
    ("4. Hybrid Strategy", scenario_hybrid),
    ("5. Category Change", scenario_category_change),
    ("6. Batch Price Update", scenario_batch_update),
    ("7. Load Test Comparison", scenario_load_comparison),
]


async def run_all() -> list[tuple[str, bool]]:
    results = []
    async with httpx.AsyncClient() as client:
        # Health check
        try:
            await api(client, "GET", "/health")
        except Exception as e:
            console.print(f"[red]Server not available: {e}[/]")
            console.print("[yellow]Start: uvicorn app.main:app --port 8020[/]")
            return []

        # Сброс в hybrid
        await set_mode(client, "hybrid")

        for name, fn in SCENARIOS:
            console.rule(f"[bold cyan]{name}")
            try:
                passed = await fn(client)
            except Exception as e:
                console.print(f"[red]  ERROR: {e}[/]")
                passed = False
            console.print(f"  Result: {status_mark(passed)}")
            results.append((name, passed))

        # Восстанавливаем hybrid
        await set_mode(client, "hybrid")

    return results


async def main():
    global BASE_URL
    if len(sys.argv) > 1:
        BASE_URL = sys.argv[1]

    console.print(Panel(
        "[bold magenta]Write-Through After Update — Test Scenarios[/]",
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
    console.print(
        f"\n[bold]Total: {total_pass}/{len(results)} PASS[/]"
    )

    if total_pass < len(results):
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
