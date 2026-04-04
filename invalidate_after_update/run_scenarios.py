"""
run_scenarios.py — 7 сценариев тестирования Invalidate After Update.

Каждый сценарий — отдельная функция, которая:
  • делает запросы к API
  • проверяет ожидаемое поведение
  • выводит результат 

Запуск:
    python run_scenarios.py                 # все сценарии
    python run_scenarios.py --scenario 3    # конкретный
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time

import httpx
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()
BASE_URL = "http://127.0.0.1:8010"


# Helpers

async def api(client: httpx.AsyncClient, method: str, path: str, **kw) -> httpx.Response:
    url = f"{BASE_URL}{path}"
    return await client.request(method, url, **kw)


async def reset(client: httpx.AsyncClient):
    """Сбросить кэш + включить invalidate."""
    await api(client, "POST", "/products/debug/flush-cache")
    await api(client, "POST", "/products/debug/toggle-invalidate?enabled=true")


def check(condition: bool, msg: str) -> bool:
    if condition:
        console.print(f"    ✅ {msg}", style="green")
    else:
        console.print(f"    ❌ {msg}", style="red bold")
    return condition


def section(title: str):
    console.print(f"\n  {title}", style="bold cyan")


# Сценарий 1: Stale cache БЕЗ invalidate

async def scenario_1(client: httpx.AsyncClient) -> bool:
    """Устаревший кэш без invalidate."""
    console.print(Panel.fit(
        "[bold red]Сценарий 1: Stale Cache (БЕЗ invalidate)[/bold red]\n"
        "Показывает проблему: без invalidate клиент получает устаревшие данные.",
        border_style="red",
    ))
    await reset(client)

    section("1. Запрашиваем товар → попадает в кэш")
    r = await api(client, "GET", "/products/1")
    old_price = r.json()["price"]
    console.print(f"    Текущая цена: {old_price}")

    section("2. Отключаем invalidate")
    await api(client, "POST", "/products/debug/toggle-invalidate?enabled=false")

    section("3. Обновляем цену в БД (без invalidate)")
    new_price = old_price + 100
    await api(client, "PUT", "/products/1", json={"price": new_price})

    section("4. Запрашиваем товар повторно")
    r = await api(client, "GET", "/products/1")
    got_price = r.json()["price"]
    console.print(f"    Получили цену: {got_price}  (ожидали новую: {new_price})")

    ok = check(
        got_price == old_price,
        f"Кэш вернул СТАРУЮ цену ({got_price}) — stale cache подтверждён!",
    )

    # Восстановить состояние
    await api(client, "POST", "/products/debug/toggle-invalidate?enabled=true")
    await api(client, "POST", "/products/debug/flush-cache")
    await api(client, "PUT", "/products/1", json={"price": old_price})

    return ok


# Сценарий 2: Корректная инвалидация

async def scenario_2(client: httpx.AsyncClient) -> bool:
    """Актуальные данные с invalidate."""
    console.print(Panel.fit(
        "[bold green]Сценарий 2: Актуальные данные С invalidate[/bold green]\n"
        "После update кэш инвалидируется → следующее чтение свежее.",
        border_style="green",
    ))
    await reset(client)

    section("1. Запрашиваем товар → кэшируется")
    r = await api(client, "GET", "/products/1")
    old_price = r.json()["price"]
    console.print(f"    Цена до обновления: {old_price}")

    section("2. Обновляем цену (invalidate включён)")
    new_price = round(old_price + 50.0, 2)
    await api(client, "PUT", "/products/1", json={"price": new_price})

    section("3. Запрашиваем товар снова")
    r = await api(client, "GET", "/products/1")
    got_price = r.json()["price"]
    console.print(f"    Получили цену: {got_price}")

    ok = check(
        got_price == new_price,
        f"Кэш обновился! Получена актуальная цена: {got_price}",
    )

    # Restore
    await api(client, "PUT", "/products/1", json={"price": old_price})
    return ok


# Сценарий 3: Инвалидация списка после обновления цены

async def scenario_3(client: httpx.AsyncClient) -> bool:
    """Инвалидация списка после обновления цены одного товара."""
    console.print(Panel.fit(
        "[bold yellow]Сценарий 3: Инвалидация списков[/bold yellow]\n"
        "Обновление одного товара должно сбросить и list, и category кэш.",
        border_style="yellow",
    ))
    await reset(client)

    section("1. Кэшируем список /products")
    r = await api(client, "GET", "/products")
    all_before = r.json()
    first_price_before = all_before[0]["price"]
    console.print(f"    Первый товар — цена: {first_price_before}")

    section("2. Кэшируем category list")
    cat = all_before[0]["category"]
    await api(client, "GET", f"/products/category/{cat}")

    section("3. Обновляем цену первого товара")
    new_price = round(first_price_before + 77.0, 2)
    await api(client, "PUT", "/products/1", json={"price": new_price})

    section("4. Проверяем /products")
    r = await api(client, "GET", "/products")
    all_after = r.json()
    got_price = all_after[0]["price"]
    ok1 = check(
        got_price == new_price,
        f"Список /products обновился: цена = {got_price}",
    )

    section("5. Проверяем /products/category/{cat}")
    r = await api(client, "GET", f"/products/category/{cat}")
    cat_products = r.json()
    cat_prices = {p["id"]: p["price"] for p in cat_products}
    ok2 = check(
        cat_prices.get(1) == new_price,
        f"Category list обновился: цена товара #1 = {cat_prices.get(1)}",
    )

    await api(client, "PUT", "/products/1", json={"price": first_price_before})
    return ok1 and ok2


# Сценарий 4: Смена категории

async def scenario_4(client: httpx.AsyncClient) -> bool:
    """Инвалидация при смене категории."""
    console.print(Panel.fit(
        "[bold magenta]Сценарий 4: Смена категории товара[/bold magenta]\n"
        "При смене категории нужно инвалидировать ОБА category-кэша.",
        border_style="magenta",
    ))
    await reset(client)

    section("1. Получаем товар #7 (iPad Pro, tablets)")
    r = await api(client, "GET", "/products/7")
    product = r.json()
    old_cat = product["category"]
    console.print(f"    Текущая категория: {old_cat}")

    section("2. Кэшируем оба списка")
    await api(client, "GET", f"/products/category/{old_cat}")
    new_cat = "laptops"
    await api(client, "GET", f"/products/category/{new_cat}")

    r_old = await api(client, "GET", f"/products/category/{old_cat}")
    old_cat_ids = [p["id"] for p in r_old.json()]
    r_new = await api(client, "GET", f"/products/category/{new_cat}")
    new_cat_ids = [p["id"] for p in r_new.json()]
    console.print(f"    {old_cat}: {len(old_cat_ids)} товаров | {new_cat}: {len(new_cat_ids)} товаров")

    section(f"3. Перемещаем товар #7 из {old_cat} → {new_cat}")
    await api(client, "PUT", "/products/7", json={"category": new_cat})

    section("4. Проверяем old category")
    r = await api(client, "GET", f"/products/category/{old_cat}")
    old_cat_ids_after = [p["id"] for p in r.json()]
    ok1 = check(
        7 not in old_cat_ids_after,
        f"Товар #7 убран из {old_cat} ({len(old_cat_ids_after)} товаров)",
    )

    section("5. Проверяем new category")
    r = await api(client, "GET", f"/products/category/{new_cat}")
    new_cat_ids_after = [p["id"] for p in r.json()]
    ok2 = check(
        7 in new_cat_ids_after,
        f"Товар #7 появился в {new_cat} ({len(new_cat_ids_after)} товаров)",
    )

    # Restore
    await api(client, "PUT", "/products/7", json={"category": old_cat})
    return ok1 and ok2


# Сценарий 5: Инвалидация статистики

async def scenario_5(client: httpx.AsyncClient) -> bool:
    """Инвалидация статистики после обновления."""
    console.print(Panel.fit(
        "[bold cyan]Сценарий 5: Инвалидация статистики[/bold cyan]\n"
        "stats:products должен обновиться после изменения цены.",
        border_style="cyan",
    ))
    await reset(client)

    section("1. Кэшируем статистику")
    r = await api(client, "GET", "/products/stats")
    stats_before = r.json()
    avg_before = stats_before["avg_price"]
    console.print(f"    Средняя цена до: {avg_before}")

    section("2. Поднимаем цену товара #1 на 1000")
    r = await api(client, "GET", "/products/1")
    old_price = r.json()["price"]
    await api(client, "PUT", "/products/1", json={"price": old_price + 1000})

    section("3. Проверяем статистику")
    r = await api(client, "GET", "/products/stats")
    stats_after = r.json()
    avg_after = stats_after["avg_price"]
    console.print(f"    Средняя цена после: {avg_after}")

    ok = check(
        avg_after > avg_before,
        f"Статистика обновилась: {avg_before} → {avg_after}",
    )

    # Restore
    await api(client, "PUT", "/products/1", json={"price": old_price})
    return ok


# Сценарий 6: Массовое обновление (скидка на категорию)

async def scenario_6(client: httpx.AsyncClient) -> bool:
    """Массовый discount + групповая инвалидация."""
    console.print(Panel.fit(
        "[bold bright_red]Сценарий 6: Массовое обновление (скидка)[/bold bright_red]\n"
        "PATCH /products/category/accessories/discount → групповая инвалидация.",
        border_style="bright_red",
    ))
    await reset(client)

    section("1. Кэшируем категорию accessories")
    r = await api(client, "GET", "/products/category/accessories")
    before = r.json()
    prices_before = {p["id"]: p["price"] for p in before}
    console.print(f"    Товаров: {len(before)}")

    section("2. Кэшируем /products и stats")
    await api(client, "GET", "/products")
    await api(client, "GET", "/products/stats")

    section("3. Кэшируем карточки")
    for pid in prices_before:
        await api(client, "GET", f"/products/{pid}")

    section("4. Применяем скидку 10%")
    await api(client, "PATCH", "/products/category/accessories/discount", json={"percent": 10})

    section("5. Проверяем category list")
    r = await api(client, "GET", "/products/category/accessories")
    after = r.json()
    prices_after = {p["id"]: p["price"] for p in after}

    all_ok = True
    for pid, old_price in prices_before.items():
        new_price = prices_after.get(pid)
        expected = round(old_price * 0.9, 2)
        ok = abs(new_price - expected) < 0.02
        check(ok, f"  Товар #{pid}: {old_price} → {new_price} (ожидали ~{expected})")
        all_ok = all_ok and ok

    section("6. Проверяем карточки товаров")
    for pid, old_price in prices_before.items():
        r = await api(client, "GET", f"/products/{pid}")
        card_price = r.json()["price"]
        expected = round(old_price * 0.9, 2)
        ok = abs(card_price - expected) < 0.02
        check(ok, f"  Карточка #{pid}: {card_price} (ожидали ~{expected})")
        all_ok = all_ok and ok

    # Restore prices
    for pid, old_price in prices_before.items():
        await api(client, "PUT", f"/products/{pid}", json={"price": old_price})

    return all_ok


# Сценарий 7: Нагрузочный мини-тест

async def scenario_7(client: httpx.AsyncClient) -> bool:
    """100 reads → 1 update → 100 reads. Проверка hit/miss/hit."""
    console.print(Panel.fit(
        "[bold bright_green]Сценарий 7: Нагрузочный мини-тест[/bold bright_green]\n"
        "100 reads → 1 update → 100 reads.\n"
        "До update: hits. После update: 1 miss, затем hits.",
        border_style="bright_green",
    ))
    await reset(client)

    N = 100
    pid = 1

    section(f"1. {N} запросов GET /products/{pid}")
    latencies_before = []
    for _ in range(N):
        t0 = time.perf_counter()
        await api(client, "GET", f"/products/{pid}")
        latencies_before.append(time.perf_counter() - t0)

    avg_before = sum(latencies_before) / len(latencies_before) * 1000
    # Первый запрос — miss (DB), остальные — hit (Redis)
    console.print(f"    Средний latency: {avg_before:.2f} ms")

    section("2. Обновляем товар")
    r = await api(client, "GET", f"/products/{pid}")
    old_price = r.json()["price"]
    new_price = round(old_price + 1, 2)
    await api(client, "PUT", f"/products/{pid}", json={"price": new_price})

    section(f"3. Ещё {N} запросов GET /products/{pid}")
    latencies_after = []
    first_after = None
    for i in range(N):
        t0 = time.perf_counter()
        r = await api(client, "GET", f"/products/{pid}")
        elapsed = time.perf_counter() - t0
        latencies_after.append(elapsed)
        if i == 0:
            first_after = r.json()["price"]

    first_latency = latencies_after[0] * 1000
    rest_avg = sum(latencies_after[1:]) / (len(latencies_after) - 1) * 1000

    ok1 = check(
        first_after == new_price,
        f"Первый запрос после update вернул актуальную цену: {first_after}",
    )

    console.print(f"    Первый запрос (miss): {first_latency:.2f} ms")
    console.print(f"    Остальные (hit): avg {rest_avg:.2f} ms")

    ok2 = check(
        first_latency > rest_avg,
        "Первый запрос медленнее (miss→DB), остальные быстрее (hit→Redis)",
    )

    table = Table(title="⏱  Latency сравнение", show_lines=True)
    table.add_column("Фаза", style="cyan")
    table.add_column("Avg latency", style="bold", justify="right")
    table.add_row("До update (hits)", f"{avg_before:.2f} ms")
    table.add_row("После update — 1-й запрос (miss)", f"{first_latency:.2f} ms")
    table.add_row("После update — остальные (hits)", f"{rest_avg:.2f} ms")
    console.print(table)

    # Restore
    await api(client, "PUT", f"/products/{pid}", json={"price": old_price})
    return ok1 and ok2


# Main

ALL_SCENARIOS = {
    1: ("Stale cache без invalidate", scenario_1),
    2: ("Актуальные данные с invalidate", scenario_2),
    3: ("Инвалидация списков после обновления", scenario_3),
    4: ("Смена категории", scenario_4),
    5: ("Инвалидация статистики", scenario_5),
    6: ("Массовое обновление (скидка)", scenario_6),
    7: ("Нагрузочный мини-тест", scenario_7),
}


async def main():
    global BASE_URL

    parser = argparse.ArgumentParser(description="Invalidate After Update — Test Scenarios")
    parser.add_argument("--scenario", type=int, default=0,
                        help="Номер сценария (0 = все)")
    parser.add_argument("--base-url", type=str, default=BASE_URL)
    args = parser.parse_args()

    BASE_URL = args.base_url

    console.print(Panel.fit(
        "[bold bright_white] Cache Invalidate After Update — Тестирование[/bold bright_white]\n\n"
        f"  Сервер: {BASE_URL}\n"
        f"  Сценариев: {len(ALL_SCENARIOS)}",
        border_style="bright_magenta",
    ))

    async with httpx.AsyncClient(timeout=30.0) as client:
        # Health check
        try:
            r = await api(client, "GET", "/health")
            if r.status_code != 200:
                raise Exception(f"Status {r.status_code}")
            console.print("  ✅ Сервер доступен\n", style="green bold")
        except Exception as e:
            console.print(f"\n  ❌ Сервер недоступен: {e}", style="red bold")
            console.print("  Запустите: cd invalidate_after_update && uvicorn app.main:app --port 8010", style="yellow")
            sys.exit(1)

        results: dict[int, bool] = {}

        if args.scenario:
            nums = [args.scenario]
        else:
            nums = list(ALL_SCENARIOS.keys())

        for num in nums:
            title, fn = ALL_SCENARIOS[num]
            try:
                ok = await fn(client)
                results[num] = ok
            except Exception as e:
                console.print(f"  ❌ Сценарий {num} провалился с ошибкой: {e}", style="red bold")
                results[num] = False

    # ── Итоги 
    console.print("\n")
    table = Table(title="📊 Итоги тестирования", show_lines=True)
    table.add_column("#", style="bold", width=4)
    table.add_column("Сценарий", style="cyan", width=40)
    table.add_column("Результат", justify="center", width=12)

    for num, ok in results.items():
        title = ALL_SCENARIOS[num][0]
        status = "✅ PASS" if ok else "❌ FAIL"
        style = "green" if ok else "red"
        table.add_row(str(num), title, f"[{style}]{status}[/{style}]")

    console.print(table)

    passed = sum(1 for v in results.values() if v)
    total = len(results)
    console.print(f"\n  Результат: {passed}/{total} сценариев пройдено\n", style="bold")


if __name__ == "__main__":
    asyncio.run(main())
