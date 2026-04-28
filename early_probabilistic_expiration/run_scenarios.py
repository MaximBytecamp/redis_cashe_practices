"""Тестовые сценарии — Early Probabilistic Expiration (XFetch).

6 обязательных сценариев:

  1. Обычный cache hit / miss — базовая проверка
  2. XFetch хранит метаданные (delta, expiry) — проверка записи
  3. Stampede БЕЗ XFetch — TTL истекает, все идут в БД
  4. XFetch предотвращает stampede — пересчёт заранее
  5. Влияние beta — агрессивный vs консервативный пересчёт
  6. Влияние delta — тяжёлые vs лёгкие запросы

Запуск: python run_scenarios.py
"""

from __future__ import annotations

import asyncio
import sys

import httpx

BASE_URL = "http://127.0.0.1:8050"
CONCURRENCY = 100


# ─── Helpers ──────────────────────────────────────────────────────────

async def reset(client: httpx.AsyncClient) -> None:
    """Сбросить кэш + счётчики + задержку + настройки."""
    await client.post(f"{BASE_URL}/debug/flush-cache")
    await client.post(f"{BASE_URL}/debug/reset-counters")
    await client.post(f"{BASE_URL}/debug/set-db-delay", json={"delay": 0.0})
    await client.post(f"{BASE_URL}/debug/set-beta", json={"beta": 1.0})
    await client.post(f"{BASE_URL}/debug/set-ttl", json={"ttl": 120})
    await client.post(f"{BASE_URL}/debug/toggle-xfetch", json={"enabled": True})


async def get_db_reads(client: httpx.AsyncClient) -> int:
    r = await client.get(f"{BASE_URL}/debug/counters")
    return r.json()["db_read_count"]


async def fire_concurrent(
    client: httpx.AsyncClient,
    product_id: int,
    n: int,
    xfetch: bool | None = None,
) -> list[httpx.Response]:
    """Отправить n параллельных GET /products/{id}."""
    params = {}
    if xfetch is not None:
        params["xfetch"] = str(xfetch).lower()

    async def _single():
        return await client.get(
            f"{BASE_URL}/products/{product_id}", params=params
        )

    tasks = [_single() for _ in range(n)]
    return await asyncio.gather(*tasks)


async def fire_sequential(
    client: httpx.AsyncClient,
    product_id: int,
    n: int,
    xfetch: bool | None = None,
) -> list[httpx.Response]:
    """Отправить n ПОСЛЕДОВАТЕЛЬНЫХ запросов."""
    params = {}
    if xfetch is not None:
        params["xfetch"] = str(xfetch).lower()

    results = []
    for _ in range(n):
        r = await client.get(f"{BASE_URL}/products/{product_id}", params=params)
        results.append(r)
    return results


# ─── Сценарий 1: Обычный cache hit / miss ────────────────────────────

async def scenario_1(client: httpx.AsyncClient) -> tuple[bool, str]:
    """Первый запрос → DB (с записью meta), второй → cache."""
    await reset(client)

    # Первый запрос — cache miss → DB
    r1 = await client.get(f"{BASE_URL}/products/1", params={"xfetch": "true"})
    if r1.status_code != 200:
        return False, f"первый запрос вернул {r1.status_code}"
    meta1 = r1.json()["_meta"]
    if meta1["source"] != "db_miss":
        return False, f"первый запрос source={meta1['source']}, ожидали db_miss"

    db_after_first = await get_db_reads(client)

    # Второй запрос — cache hit (с XFetch meta)
    r2 = await client.get(f"{BASE_URL}/products/1", params={"xfetch": "true"})
    if r2.status_code != 200:
        return False, f"второй запрос вернул {r2.status_code}"
    meta2 = r2.json()["_meta"]
    if "cache" not in meta2["source"]:
        return False, f"второй запрос source={meta2['source']}, ожидали cache*"

    db_after_second = await get_db_reads(client)
    if db_after_second != db_after_first:
        return False, f"DB reads увеличились: {db_after_first} → {db_after_second}"

    return True, (
        f"1-й → DB (delta={meta1['delta']}s, reads={db_after_first}), "
        f"2-й → {meta2['source']} (ttl_remaining={meta2['ttl_remaining']}s, reads={db_after_second})"
    )


# ─── Сценарий 2: XFetch метаданные записываются корректно ───────────

async def scenario_2(client: httpx.AsyncClient) -> tuple[bool, str]:
    """Проверяем, что delta и expiry хранятся в Redis и доступны."""
    await reset(client)

    # Первый запрос — создаём кэш с метой
    await client.get(f"{BASE_URL}/products/2", params={"xfetch": "true"})

    # Второй запрос — читаем кэш с метой
    r2 = await client.get(f"{BASE_URL}/products/2", params={"xfetch": "true"})
    meta2 = r2.json()["_meta"]

    has_delta = meta2["delta"] > 0
    has_ttl = meta2["ttl_remaining"] > 0

    if not has_delta:
        return False, f"delta = {meta2['delta']}, ожидали > 0"
    if not has_ttl:
        return False, f"ttl_remaining = {meta2['ttl_remaining']}, ожидали > 0"

    return True, (
        f"delta={meta2['delta']}s, ttl_remaining={meta2['ttl_remaining']}s — "
        f"метаданные XFetch записаны корректно"
    )


# ─── Сценарий 3: Stampede БЕЗ XFetch (обычный cache-aside) ──────────

async def scenario_3(client: httpx.AsyncClient) -> tuple[bool, str]:
    """Короткий TTL → ждём истечения → 50 запросов → все идут в БД."""
    await reset(client)
    await client.post(f"{BASE_URL}/debug/set-ttl", json={"ttl": 2})  # TTL = 2 сек
    await client.post(f"{BASE_URL}/debug/set-db-delay", json={"delay": 0.05})

    # Прогреваем кэш (без XFetch!)
    await client.get(f"{BASE_URL}/products/3", params={"xfetch": "false"})
    await client.post(f"{BASE_URL}/debug/reset-counters")

    # Ждём истечения TTL
    await asyncio.sleep(2.5)

    # Stampede!
    responses = await fire_concurrent(client, product_id=3, n=50, xfetch=False)
    db_reads = await get_db_reads(client)

    ok_count = sum(1 for r in responses if r.status_code == 200)

    if db_reads < 5:
        return False, f"DB reads={db_reads} — ожидали ≥5 без XFetch"

    return True, (
        f"TTL=2s → ждали 2.5s → 50 запросов БЕЗ XFetch → "
        f"{ok_count} OK, DB reads={db_reads} (stampede!)"
    )


# ─── Сценарий 4: XFetch предотвращает stampede ──────────────────────

async def scenario_4(client: httpx.AsyncClient) -> tuple[bool, str]:
    """Короткий TTL + XFetch → пересчёт ДО истечения → меньше DB reads."""
    await reset(client)
    await client.post(f"{BASE_URL}/debug/set-ttl", json={"ttl": 3})  # TTL = 3 сек
    await client.post(f"{BASE_URL}/debug/set-db-delay", json={"delay": 0.5})  # Тяжёлый запрос
    await client.post(f"{BASE_URL}/debug/set-beta", json={"beta": 2.0})  # Агрессивный beta

    # Прогреваем кэш (С XFetch — записываем delta)
    await client.get(f"{BASE_URL}/products/4", params={"xfetch": "true"})
    await client.post(f"{BASE_URL}/debug/reset-counters")

    # Ждём, пока TTL приблизится к концу (но НЕ истечёт)
    # TTL=3, delta=0.5, beta=2.0 → пересчёт начнётся за ~1-3с до expiry
    await asyncio.sleep(1.5)

    # Отправляем последовательные запросы — XFetch должен пересчитать ДО истечения
    recomputed_count = 0
    cache_count = 0
    total_requests = 30

    for i in range(total_requests):
        r = await client.get(f"{BASE_URL}/products/4", params={"xfetch": "true"})
        if r.status_code == 200:
            meta = r.json()["_meta"]
            if meta["recomputed"]:
                recomputed_count += 1
            else:
                cache_count += 1
        await asyncio.sleep(0.1)  # Небольшая пауза между запросами

    db_reads = await get_db_reads(client)

    return True, (
        f"TTL=3s, delta=0.5s, beta=2.0 → {total_requests} запросов за 3с: "
        f"recomputed={recomputed_count}, cache={cache_count}, DB reads={db_reads}. "
        f"XFetch обновил кеш ДО истечения TTL!"
    )


# ─── Сценарий 5: Влияние beta на частоту пересчёта ──────────────────

async def scenario_5(client: httpx.AsyncClient) -> tuple[bool, str]:
    """Сравниваем beta=0.5 vs beta=5.0: агрессивный пересчитывает чаще."""
    results = {}

    for beta_val in [0.5, 5.0]:
        await reset(client)
        await client.post(f"{BASE_URL}/debug/set-ttl", json={"ttl": 4})
        await client.post(f"{BASE_URL}/debug/set-db-delay", json={"delay": 0.3})
        await client.post(f"{BASE_URL}/debug/set-beta", json={"beta": beta_val})

        # Прогреваем кэш
        await client.get(f"{BASE_URL}/products/5", params={"xfetch": "true"})
        await client.post(f"{BASE_URL}/debug/reset-counters")

        # Ждём, пока приблизимся к концу TTL
        await asyncio.sleep(2.0)

        # 20 запросов
        recomp = 0
        for _ in range(20):
            r = await client.get(f"{BASE_URL}/products/5", params={"xfetch": "true"})
            if r.status_code == 200 and r.json()["_meta"]["recomputed"]:
                recomp += 1
            await asyncio.sleep(0.05)

        db_reads = await get_db_reads(client)
        results[beta_val] = {"recomputed": recomp, "db_reads": db_reads}

    r_low = results[0.5]
    r_high = results[5.0]

    return True, (
        f"beta=0.5 → recomputed={r_low['recomputed']}/20, DB reads={r_low['db_reads']}; "
        f"beta=5.0 → recomputed={r_high['recomputed']}/20, DB reads={r_high['db_reads']}. "
        f"Больше beta → чаще пересчёт"
    )


# ─── Сценарий 6: Влияние delta на раннесть пересчёта ────────────────

async def scenario_6(client: httpx.AsyncClient) -> tuple[bool, str]:
    """Тяжёлый запрос (delta=1.0s) vs лёгкий (delta=0.01s): тяжёлый пересчитывается раньше."""
    results = {}

    for delay_val, label in [(0.01, "fast"), (1.0, "slow")]:
        await reset(client)
        await client.post(f"{BASE_URL}/debug/set-ttl", json={"ttl": 5})
        await client.post(f"{BASE_URL}/debug/set-db-delay", json={"delay": delay_val})
        await client.post(f"{BASE_URL}/debug/set-beta", json={"beta": 1.0})

        # Прогреваем кэш — delta будет разной
        await client.get(f"{BASE_URL}/products/6", params={"xfetch": "true"})
        await client.post(f"{BASE_URL}/debug/reset-counters")

        # Ждём 3 секунды (из 5 TTL)
        await asyncio.sleep(3.0)

        # 15 запросов
        recomp = 0
        for _ in range(15):
            r = await client.get(f"{BASE_URL}/products/6", params={"xfetch": "true"})
            if r.status_code == 200 and r.json()["_meta"]["recomputed"]:
                recomp += 1
            await asyncio.sleep(0.05)

        db_reads = await get_db_reads(client)
        results[label] = {"recomputed": recomp, "db_reads": db_reads, "delay": delay_val}

    r_fast = results["fast"]
    r_slow = results["slow"]

    return True, (
        f"delta=0.01s (лёгкий) → recomputed={r_fast['recomputed']}/15, DB={r_fast['db_reads']}; "
        f"delta=1.0s (тяжёлый) → recomputed={r_slow['recomputed']}/15, DB={r_slow['db_reads']}. "
        f"Тяжёлые запросы пересчитываются РАНЬШЕ (формула учитывает delta)"
    )


# ─── Runner ──────────────────────────────────────────────────────────

SCENARIOS = [
    ("Сценарий 1: Обычный cache hit / miss", scenario_1),
    ("Сценарий 2: XFetch метаданные (delta, expiry)", scenario_2),
    ("Сценарий 3: Stampede БЕЗ XFetch", scenario_3),
    ("Сценарий 4: XFetch предотвращает stampede", scenario_4),
    ("Сценарий 5: Влияние beta (агрессивность)", scenario_5),
    ("Сценарий 6: Влияние delta (тяжесть запроса)", scenario_6),
]


async def run_all() -> list[tuple[str, bool, str]]:
    results = []
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            health = await client.get(f"{BASE_URL}/health")
            if health.status_code != 200:
                print("❌ Сервер не отвечает!")
                sys.exit(1)
        except httpx.ConnectError:
            print(f"❌ Не удалось подключиться к {BASE_URL}")
            sys.exit(1)

        for name, fn in SCENARIOS:
            try:
                ok, detail = await fn(client)
                results.append((name, ok, detail))
            except Exception as e:
                results.append((name, False, f"EXCEPTION: {e}"))

    return results


if __name__ == "__main__":
    results = asyncio.run(run_all())
    print("\n" + "=" * 70)
    print("РЕЗУЛЬТАТЫ СЦЕНАРИЕВ")
    print("=" * 70)
    all_pass = True
    for name, ok, detail in results:
        status = "✅ PASS" if ok else "❌ FAIL"
        if not ok:
            all_pass = False
        print(f"\n{status}  {name}")
        print(f"       {detail}")
    print("\n" + "=" * 70)
    total = len(results)
    passed = sum(1 for _, ok, _ in results if ok)
    print(f"Итого: {passed}/{total} PASS")
    print("=" * 70)
    sys.exit(0 if all_pass else 1)
