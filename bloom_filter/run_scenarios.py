#!/usr/bin/env python3
"""
run_scenarios.py — Тестовые сценарии Bloom Filter + Negative Cache.

Сценарии:
  1. Обычный cache hit/miss        — существующий товар кешируется
  2. Bloom filter отсекает мусор   — несуществующий ID → мгновенный 404
  3. Bloom false positive + neg    — bloom пропускает, но neg cache ловит
  4. Без защиты: все в БД          — bloom=off, neg=off → N DB reads
  5. Bloom + создание товара       — новый товар добавляется в bloom
  6. Bloom после удаления          — bloom всё ещё "maybe" (нельзя удалять!)
  7. Массовая атака: 200 рандомных — bloom отсекает большинство
  8. Сравнение 3 режимов           — без защиты / bloom / bloom+neg
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
BASE_URL = "http://127.0.0.1:8060"
TIMEOUT = httpx.Timeout(15.0)


# ── Helpers ──────────────────────────────────────────────────


async def api(client: httpx.AsyncClient, method: str, path: str, **kwargs) -> httpx.Response:
    return await client.request(method, f"{BASE_URL}{path}", timeout=TIMEOUT, **kwargs)


async def api_json(client: httpx.AsyncClient, method: str, path: str, **kwargs) -> dict:
    resp = await api(client, method, path, **kwargs)
    resp.raise_for_status()
    return resp.json()


async def flush(client: httpx.AsyncClient) -> None:
    await api_json(client, "POST", "/api/debug/flush-cache")


async def reset(client: httpx.AsyncClient) -> None:
    await api_json(client, "POST", "/api/debug/reset-counters")


async def counters(client: httpx.AsyncClient) -> dict:
    return await api_json(client, "GET", "/api/debug/counters")


async def set_bloom(client: httpx.AsyncClient, enabled: bool) -> None:
    await api_json(client, "POST", f"/api/debug/set-bloom?enabled={str(enabled).lower()}")


async def set_negative(client: httpx.AsyncClient, enabled: bool) -> None:
    await api_json(client, "POST", f"/api/debug/set-negative-cache?enabled={str(enabled).lower()}")


async def rebuild_bloom(client: httpx.AsyncClient) -> dict:
    return await api_json(client, "POST", "/api/debug/rebuild-bloom")


def mark(passed: bool) -> str:
    return "[bold green]✅ PASS[/]" if passed else "[bold red]❌ FAIL[/]"


# ── Scenario 1: Обычный cache hit / miss ─────────────────────


async def scenario_1(client: httpx.AsyncClient) -> tuple[bool, str]:
    """Существующий товар: 1-й → DB, 2-й → cache."""
    await flush(client)
    await set_bloom(client, True)
    await set_negative(client, True)
    await reset(client)

    r1 = await api(client, "GET", "/api/products/1")
    d1 = r1.json()
    r2 = await api(client, "GET", "/api/products/1")
    d2 = r2.json()

    c = await counters(client)

    ok = (d1["_source"] == "db" and d2["_source"] == "cache"
          and c["db_read"] == 1 and c["cache_hit"] == 1)

    detail = (f"1-й source={d1['_source']}, 2-й source={d2['_source']}, "
              f"db_read={c['db_read']}, cache_hit={c['cache_hit']}")
    return ok, detail


# ── Scenario 2: Bloom отсекает несуществующий ID ──────────────


async def scenario_2(client: httpx.AsyncClient) -> tuple[bool, str]:
    """ID=999999 точно не в Bloom → 404 без DB и без Redis cache."""
    await flush(client)
    await set_bloom(client, True)
    await set_negative(client, True)
    await reset(client)

    N = 20
    for _ in range(N):
        await api(client, "GET", "/api/products/999999")

    c = await counters(client)

    ok = c["bloom_reject"] == N and c["db_read"] == 0 and c["neg_hit"] == 0
    detail = (f"{N} запросов → bloom_reject={c['bloom_reject']}, "
              f"db_read={c['db_read']}, neg_hit={c['neg_hit']}")
    return ok, detail


# ── Scenario 3: False positive + negative cache ──────────────


async def scenario_3(client: httpx.AsyncClient) -> tuple[bool, str]:
    """Удалённый товар: bloom говорит 'maybe', но neg cache ловит."""
    await flush(client)
    await set_bloom(client, True)
    await set_negative(client, True)
    await reset(client)

    # Создаём и удаляем товар — он останется в Bloom!
    await api(client, "POST", "/api/products?product_id=5000", json={
        "name": "Temp Product", "price": 10.0, "category": "test", "stock": 1,
    })
    await api(client, "DELETE", "/api/products/5000")
    # Очищаем neg cache, чтобы первый GET пошёл в DB (а не в neg cache от DELETE)
    await flush(client)
    await reset(client)

    # Первый запрос: bloom=maybe → neg=miss → cache=miss → DB=not_found → neg set
    await api(client, "GET", "/api/products/5000")
    await counters(client)

    # Остальные: bloom=maybe → neg=hit → 404 без DB
    for _ in range(19):
        await api(client, "GET", "/api/products/5000")
    c2 = await counters(client)

    ok = (c2["db_read"] == 1 and c2["neg_hit"] == 19
          and c2["bloom_pass"] == 20 and c2["bloom_reject"] == 0)

    detail = (f"bloom_pass={c2['bloom_pass']}, db_read={c2['db_read']}, "
              f"neg_hit={c2['neg_hit']} — negative cache поймал 19 из 20")
    return ok, detail


# ── Scenario 4: Без защиты — все в БД ────────────────────────


async def scenario_4(client: httpx.AsyncClient) -> tuple[bool, str]:
    """bloom=off, neg=off → каждый запрос к несуществующему ID идёт в БД."""
    await flush(client)
    await set_bloom(client, False)
    await set_negative(client, False)
    await reset(client)

    N = 20
    for _ in range(N):
        await api(client, "GET", "/api/products/888888")

    c = await counters(client)

    ok = c["db_read"] == N and c["bloom_reject"] == 0 and c["neg_hit"] == 0
    detail = (f"{N} запросов → db_read={c['db_read']} (все пробили в БД!), "
              f"bloom_reject={c['bloom_reject']}, neg_hit={c['neg_hit']}")

    # Восстановить
    await set_bloom(client, True)
    await set_negative(client, True)
    return ok, detail


# ── Scenario 5: Создание товара → добавление в Bloom ─────────


async def scenario_5(client: httpx.AsyncClient) -> tuple[bool, str]:
    """Новый товар автоматически добавляется в Bloom filter."""
    await flush(client)
    await set_bloom(client, True)
    await set_negative(client, True)
    await reset(client)

    new_id = 6000

    # До создания: bloom должен отсечь
    await api(client, "GET", f"/api/products/{new_id}")
    c1 = await counters(client)
    rejected_before = c1["bloom_reject"]

    # Создаём
    await api(client, "POST", f"/api/products?product_id={new_id}", json={
        "name": "New Bloom Product", "price": 99.0, "category": "test", "stock": 5,
    })
    await flush(client)
    await reset(client)

    # После создания: bloom пропускает, данные из DB → cache
    r2 = await api(client, "GET", f"/api/products/{new_id}")
    d2 = r2.json()
    c2 = await counters(client)

    ok = rejected_before >= 1 and c2["bloom_pass"] >= 1 and d2["_source"] in ("db", "cache")
    detail = (f"До создания: bloom_reject={rejected_before}. "
              f"После: bloom_pass={c2['bloom_pass']}, source={d2['_source']}")

    # Cleanup
    await api(client, "DELETE", f"/api/products/{new_id}")
    return ok, detail


# ── Scenario 6: Удалённый товар остаётся в Bloom ─────────────


async def scenario_6(client: httpx.AsyncClient) -> tuple[bool, str]:
    """После удаления Bloom всё ещё говорит 'maybe' (нельзя удалять из Bloom!)."""
    await flush(client)
    await set_bloom(client, True)
    await set_negative(client, True)
    await reset(client)

    # Товар id=1 существует и в Bloom
    # Удаляем
    # Заранее сохраним что id=1 точно в bloom
    debug_before = await api_json(client, "GET", "/api/debug/product/1")
    bloom_before = debug_before["bloom_filter"]

    # Тут мы не можем удалить id=1 (он seed), создадим временный
    await api(client, "POST", "/api/products?product_id=7000", json={
        "name": "Will Delete", "price": 5.0, "category": "test", "stock": 1,
    })
    await api(client, "DELETE", "/api/products/7000")

    # Проверяем: bloom всё ещё говорит "maybe" для 7000
    debug_after = await api_json(client, "GET", "/api/debug/product/7000")
    bloom_after = debug_after["bloom_filter"]

    ok = bloom_before == "maybe_exists" and bloom_after == "maybe_exists"
    detail = (f"До удаления: bloom={bloom_before}. "
              f"После удаления: bloom={bloom_after} — удалить из Bloom НЕЛЬЗЯ!")
    return ok, detail


# ── Scenario 7: Массовая атака рандомными ID ─────────────────


async def scenario_7(client: httpx.AsyncClient) -> tuple[bool, str]:
    """200 рандомных ID: Bloom отсечёт большинство."""
    await flush(client)
    await set_bloom(client, True)
    await set_negative(client, True)
    await rebuild_bloom(client)
    await reset(client)

    N = 200
    tasks = []
    for i in range(N):
        rid = 100_000 + i  # гарантированно не в БД
        tasks.append(api(client, "GET", f"/api/products/{rid}"))
    await asyncio.gather(*tasks)

    c = await counters(client)

    # Большинство должны быть bloom_reject (зависит от FP rate)
    reject_pct = c["bloom_reject"] / N * 100 if N else 0

    ok = c["bloom_reject"] > N * 0.8  # >80% отсечено bloom
    detail = (f"{N} рандомных ID → bloom_reject={c['bloom_reject']} ({reject_pct:.0f}%), "
              f"db_read={c['db_read']}, neg_hit={c['neg_hit']}, "
              f"bloom FP={c['bloom_pass']}")
    return ok, detail


# ── Scenario 8: Сравнение трёх режимов ────────────────────────


async def scenario_8(client: httpx.AsyncClient) -> tuple[bool, str]:
    """Сравнение: без защиты vs bloom only vs bloom+neg."""
    N = 30
    test_ids = list(range(200_000, 200_000 + N))
    results = {}

    for mode, bloom_on, neg_on in [
        ("no_protection", False, False),
        ("bloom_only", True, False),
        ("bloom_and_neg", True, True),
    ]:
        await flush(client)
        await set_bloom(client, bloom_on)
        await set_negative(client, neg_on)
        if bloom_on:
            await rebuild_bloom(client)
        await reset(client)

        t0 = time.perf_counter()
        # Два прохода: первый заполняет neg cache, второй использует
        for _ in range(2):
            for rid in test_ids:
                await api(client, "GET", f"/api/products/{rid}")
        elapsed = (time.perf_counter() - t0) * 1000

        c = await counters(client)
        results[mode] = {
            "db_reads": c["db_read"],
            "bloom_reject": c["bloom_reject"],
            "neg_hit": c["neg_hit"],
            "elapsed_ms": round(elapsed, 1),
        }

    # Таблица
    table = Table(title=f"Сравнение: {N} ID × 2 прохода = {N * 2} запросов", border_style="cyan")
    table.add_column("Metric", style="bold")
    table.add_column("Без защиты", justify="right")
    table.add_column("Bloom only", justify="right")
    table.add_column("Bloom+Neg", justify="right")

    r0 = results["no_protection"]
    r1 = results["bloom_only"]
    r2 = results["bloom_and_neg"]

    table.add_row("DB reads", str(r0["db_reads"]), str(r1["db_reads"]), str(r2["db_reads"]))
    table.add_row("Bloom reject", str(r0["bloom_reject"]), str(r1["bloom_reject"]), str(r2["bloom_reject"]))
    table.add_row("Neg cache hit", str(r0["neg_hit"]), str(r1["neg_hit"]), str(r2["neg_hit"]))
    table.add_row("Time ms", str(r0["elapsed_ms"]), str(r1["elapsed_ms"]), str(r2["elapsed_ms"]))

    console.print(table)

    # Bloom+neg должен иметь минимум DB reads
    ok = r2["db_reads"] <= r1["db_reads"] <= r0["db_reads"]
    detail = (f"DB reads: без={r0['db_reads']}, bloom={r1['db_reads']}, "
              f"bloom+neg={r2['db_reads']}")

    await set_bloom(client, True)
    await set_negative(client, True)
    return ok, detail


# ── Runner ───────────────────────────────────────────────────

SCENARIOS = [
    ("Сценарий 1: Обычный cache hit / miss", scenario_1),
    ("Сценарий 2: Bloom filter отсекает мусор", scenario_2),
    ("Сценарий 3: False positive + negative cache", scenario_3),
    ("Сценарий 4: Без защиты — все в БД", scenario_4),
    ("Сценарий 5: Создание товара → Bloom add", scenario_5),
    ("Сценарий 6: Удалённый товар остаётся в Bloom", scenario_6),
    ("Сценарий 7: Массовая атака (200 рандомных ID)", scenario_7),
    ("Сценарий 8: Сравнение трёх режимов", scenario_8),
]


async def run_all() -> list[tuple[str, bool, str]]:
    results = []
    async with httpx.AsyncClient() as client:
        try:
            r = await api(client, "GET", "/health")
            r.raise_for_status()
        except Exception as e:
            console.print(f"[red]Server not available: {e}[/]")
            return []

        for name, fn in SCENARIOS:
            console.rule(f"[bold cyan]{name}")
            try:
                passed, detail = await fn(client)
            except Exception as e:
                console.print(f"[red]  ERROR: {e}[/]")
                import traceback
                traceback.print_exc()
                passed, detail = False, str(e)
            console.print(f"  {mark(passed)} {detail}")
            results.append((name, passed, detail))

    return results


async def main():
    if len(sys.argv) > 1:
        global BASE_URL
        BASE_URL = sys.argv[1]

    console.print(Panel(
        "[bold magenta]Bloom Filter + Negative Cache — Test Scenarios[/]",
        subtitle=f"target: {BASE_URL}",
    ))

    results = await run_all()
    if not results:
        sys.exit(1)

    console.print()
    t = Table(title="Summary", border_style="green")
    t.add_column("Сценарий", style="bold")
    t.add_column("Результат", justify="center")
    total_pass = 0
    for name, passed, _ in results:
        t.add_row(name, mark(passed))
        if passed:
            total_pass += 1
    console.print(t)
    console.print(f"\n[bold]{total_pass}/{len(results)} PASS[/]")

    if total_pass < len(results):
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
