#!/usr/bin/env python3
"""
Early Probabilistic Expiration (XFetch) — полный демо-раннер.

Запускает сервер → выполняет 6 сценариев → сравнительная таблица → аналитический отчёт.

Использование:
    python run_demo.py
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time

import httpx
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from run_scenarios import SCENARIOS, get_db_reads, reset

console = Console(width=110)
BASE_URL = "http://127.0.0.1:8050"
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))


# ─── ASCII-Архитектура ───────────────────────────────────────────────

ARCHITECTURE = r"""
┌─────────────────────────────────────────────────────────────────────────────────────┐
│              Early Probabilistic Expiration (XFetch)                                 │
│                                                                                     │
│   Проблема: TTL истекает → 1000 запросов → ВСЕ идут в БД (stampede)                │
│   Решение:  Пересчитать кеш ЗАРАНЕЕ, ДО истечения TTL, ВЕРОЯТНОСТНО                │
│                                                                                     │
│   Client                                                                            │
│        │                                                                            │
│        ▼                                                                            │
│   ┌─────────────────────────────────────────────────────────┐                       │
│   │  FastAPI  GET /products/{id}?xfetch=true                │                       │
│   │           ┌─────────────────────────────────────────┐   │                       │
│   │           │  product_service.get_product()           │   │                       │
│   │           │    ↓ xfetch=true?                        │   │                       │
│   │           │    ├─ NO  → get_product_no_xfetch()      │   │  ← ждёт истечения    │
│   │           │    └─ YES → get_product_with_xfetch()    │   │  ← пересчёт заранее  │
│   │           └─────────────────────────────────────────┘   │                       │
│   └──────────────────────┬──────────────────────────────────┘                       │
│                          │                                                          │
│              ┌───────────▼───────────┐                                              │
│              │   early_expiration    │                                               │
│              │   _service.py         │                                               │
│              │                       │                                               │
│              │  1. xfetch_get(key)   │──── MISS ──→ DB read → xfetch_set()          │
│              │     ↓ HIT             │                                               │
│              │  2. Читаем delta,     │                                               │
│              │     expiry из meta    │                                               │
│              │     ↓                 │                                               │
│              │  3. Формула XFetch:   │                                               │
│              │     now > expiry -    │                                               │
│              │     delta×β×(-ln(r))  │                                               │
│              │     ↓                 │                                               │
│              │     ├─ YES (пора!) ───│──→ DB read → xfetch_set() (обновляем кеш)    │
│              │     │   шанс растёт   │                                               │
│              │     │   ближе к expiry│                                               │
│              │     │                 │                                               │
│              │     └─ NO (ещё рано) ─│──→ return cached data                        │
│              └───────────────────────┘                                               │
│                     │           │                                                    │
│          ┌──────────▼──┐  ┌────▼────────┐                                           │
│          │   Redis     │  │  SQLite     │                                            │
│          │   DB=5      │  │  (products) │                                            │
│          │             │  └─────────────┘                                            │
│          │  product:1  │  ← данные                                                  │
│          │  meta:      │  ← {delta: 0.035, expiry: 1713550920.0}                    │
│          │  product:1  │                                                             │
│          └─────────────┘                                                             │
│                                                                                     │
│   Формула:  should_recompute = now - (expiry - delta × β × (-ln(random()))) > 0    │
│                                                                                     │
│   delta большой (тяжёлый запрос) → пересчёт раньше  (больше запас)                  │
│   beta  большой (агрессивный)    → пересчёт раньше  (чаще обновление)               │
│   random()                       → вероятностный     (не все сразу)                  │
└─────────────────────────────────────────────────────────────────────────────────────┘
"""


# ─── Helpers ──────────────────────────────────────────────────────────

def print_header():
    console.print()
    console.print(
        Panel(
            "[bold cyan]Early Probabilistic Expiration (XFetch)[/]\n"
            "[dim]Проект №6 — Вероятностный пересчёт кэша ДО истечения TTL[/]",
            border_style="bright_blue",
            padding=(1, 4),
        )
    )


def print_architecture():
    console.print()
    console.print(
        Panel(
            ARCHITECTURE,
            title="[bold green]Архитектурная схема[/]",
            border_style="green",
        )
    )


async def wait_for_server(url: str, timeout: float = 15.0) -> bool:
    t0 = time.time()
    async with httpx.AsyncClient() as client:
        while time.time() - t0 < timeout:
            try:
                r = await client.get(url)
                if r.status_code == 200:
                    return True
            except (httpx.ConnectError, httpx.ReadError):
                pass
            await asyncio.sleep(0.3)
    return False


# ─── Нагрузочное сравнение ───────────────────────────────────────────

async def run_comparison() -> dict:
    """Два замера: без XFetch и с XFetch — имитация истечения TTL."""
    results = {}

    async with httpx.AsyncClient(timeout=30.0) as client:
        for mode_label, use_xfetch in [("Без XFetch", False), ("С XFetch", True)]:
            await reset(client)
            await client.post(f"{BASE_URL}/debug/set-ttl", json={"ttl": 3})
            await client.post(f"{BASE_URL}/debug/set-db-delay", json={"delay": 0.1})
            await client.post(f"{BASE_URL}/debug/set-beta", json={"beta": 2.0})

            # Прогреваем кэш
            await client.get(f"{BASE_URL}/products/1", params={"xfetch": str(use_xfetch).lower()})
            await client.post(f"{BASE_URL}/debug/reset-counters")

            # Ждём приближения к TTL
            await asyncio.sleep(2.0)

            # 50 последовательных запросов за 2.5 секунды (пересекаем TTL)
            t0 = time.perf_counter()
            recomputed = 0
            cache_hits = 0
            db_misses = 0

            for _ in range(50):
                r = await client.get(
                    f"{BASE_URL}/products/1",
                    params={"xfetch": str(use_xfetch).lower()},
                )
                if r.status_code == 200:
                    meta = r.json()["_meta"]
                    if meta.get("recomputed"):
                        recomputed += 1
                    elif "cache" in meta["source"]:
                        cache_hits += 1
                    else:
                        db_misses += 1
                await asyncio.sleep(0.05)

            elapsed = time.perf_counter() - t0
            db_reads = await get_db_reads(client)

            results[mode_label] = {
                "db_reads": db_reads,
                "recomputed": recomputed,
                "cache_hits": cache_hits,
                "db_misses": db_misses,
                "time": elapsed,
            }

    return results


# ─── Аналитический отчёт ─────────────────────────────────────────────

REPORT_QUESTIONS = [
    (
        "1. Что такое Early Probabilistic Expiration (XFetch)?",
        "XFetch — алгоритм, при котором каждый запрос к кешу с ВЕРОЯТНОСТЬЮ\n"
        "решает: «не пора ли мне обновить эти данные?». Вероятность растёт\n"
        "по мере приближения к моменту истечения TTL.\n"
        "Результат: кто-то ОДИН обновляет кеш ДО того, как TTL истечёт,\n"
        "и stampede не происходит."
    ),
    (
        "2. Как работает формула XFetch?",
        "should_recompute = now > expiry - delta × β × (-ln(random()))\n\n"
        "• now    — текущее время\n"
        "• expiry — когда ключ истечёт\n"
        "• delta  — время вычисления (сколько длился DB read)\n"
        "• β      — коэффициент агрессивности (обычно 1.0)\n"
        "• random() — случайное число (0, 1]\n\n"
        "-ln(random()) даёт экспоненциальное распределение со средним = 1.\n"
        "Чем ближе now к expiry, тем чаще выражение > 0 (пора обновлять).\n"
        "Чем больше delta (тяжёлый запрос), тем раньше начнётся пересчёт."
    ),
    (
        "3. Чем XFetch отличается от mutex lock?",
        "Mutex lock — РЕАКТИВНЫЙ: stampede уже произошёл, lock ограничивает\n"
        "  до 1 похода в БД. Остальные ЖДУТ.\n\n"
        "XFetch — ПРЕВЕНТИВНЫЙ: stampede НЕ происходит, потому что кеш\n"
        "  обновляется ДО истечения TTL. Никто не ждёт.\n\n"
        "• Mutex lock: 1 DB read, но 99 потоков BLOCKED (ждут 80ms × retry)\n"
        "• XFetch: 1-3 DB reads, но 0 потоков blocked (все читают из кеша)\n\n"
        "XFetch лучше по latency, mutex lock — по гарантиям."
    ),
    (
        "4. Зачем нужен параметр beta (β)?",
        "β контролирует агрессивность пересчёта:\n"
        "• β = 0.5 → консервативный: пересчёт начинается очень близко к TTL\n"
        "• β = 1.0 → стандартный: хороший баланс\n"
        "• β = 5.0 → агрессивный: пересчёт начинается задолго до TTL\n\n"
        "Высокий β = чаще DB reads, но меньше шанс stampede.\n"
        "Низкий β = реже DB reads, но выше шанс stampede.\n"
        "Сценарий 5 демонстрирует разницу."
    ),
    (
        "5. Почему delta (время вычисления) влияет на раннесть пересчёта?",
        "Если запрос в БД занимает 2 секунды (delta=2s), то нужен БОЛЬШОЙ запас\n"
        "до истечения TTL. Иначе запрос не успеет завершиться до expiry.\n\n"
        "Если запрос занимает 0.01s (delta=0.01), можно обновлять в последний момент.\n\n"
        "Формула автоматически учитывает это:\n"
        "  delta=2.0, β=1.0 → пересчёт начнётся за ~2-20с до expiry\n"
        "  delta=0.01, β=1.0 → пересчёт начнётся за ~0.01-0.1с до expiry\n"
        "Сценарий 6 демонстрирует разницу."
    ),
    (
        "6. Какие недостатки у XFetch?",
        "1. Не гарантирует ровно 1 DB read — несколько запросов могут\n"
        "   одновременно решить «пора обновлять». Обычно 1-5 reads.\n"
        "2. Лишние DB reads при высоком β — слишком ранний пересчёт.\n"
        "3. Требует хранить метаданные (delta, expiry) — дополнительные ключи.\n"
        "4. Не помогает при первом cache miss — только при TTL expiry.\n\n"
        "Production: часто комбинируют XFetch + mutex lock:\n"
        "  XFetch для горячих ключей (превентивно),\n"
        "  mutex lock как fallback при полном miss."
    ),
    (
        "7. Чем XFetch отличается от TTL jitter?",
        "• TTL jitter — сдвигает МОМЕНТ истечения: вместо T=120 для всех,\n"
        "  каждый ключ истекает в T=120±10. Защищает от AVALANCHE\n"
        "  (одновременного истечения МНОГИХ ключей).\n\n"
        "• XFetch — обновляет кеш ДО истечения: ключ живёт до T=120,\n"
        "  но при T≈115 кто-то уже обновляет. Защищает от STAMPEDE\n"
        "  (множества запросов к ОДНОМУ истекшему ключу).\n\n"
        "В production используют ОБА: TTL jitter + XFetch."
    ),
    (
        "8. Когда использовать XFetch в production?",
        "Идеально подходит для:\n"
        "• Горячие ключи с предсказуемым трафиком (главная страница, каталог)\n"
        "• Тяжёлые запросы (JOIN, агрегации) — delta большой, XFetch даёт запас\n"
        "• Системы где latency критична — нет блокировок как в mutex lock\n\n"
        "НЕ подходит для:\n"
        "• Редко запрашиваемые ключи — некому обновлять до expiry\n"
        "• Данные, которые должны быть строго актуальны — XFetch отдаёт\n"
        "  «почти истёкшие» данные пока обновляет\n"
        "• Первый cache miss — XFetch не поможет, нужен mutex lock"
    ),
]


# ─── Main ─────────────────────────────────────────────────────────────

async def main():
    print_header()
    print_architecture()

    # ── Запуск сервера ──
    console.print("\n[bold yellow]▶ Запуск сервера...[/]")
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    server_proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", "8050"],
        cwd=PROJECT_DIR,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        if not await wait_for_server(f"{BASE_URL}/health"):
            console.print("[bold red]✗ Сервер не запустился за 15 секунд![/]")
            server_proc.kill()
            sys.exit(1)
        console.print("[green]✓ Сервер запущен на порту 8050[/]\n")

        # ── Выполнение сценариев ──
        console.print(
            Panel(
                "[bold]Запуск 6 тестовых сценариев[/]",
                border_style="bright_magenta",
            )
        )

        all_pass = True
        scenario_results = []

        async with httpx.AsyncClient(timeout=30.0) as client:
            for i, (name, fn) in enumerate(SCENARIOS, 1):
                console.print(f"\n[bold cyan]{'─' * 90}[/]")
                console.print(f"[bold]  {name}[/]")
                console.print(f"[bold cyan]{'─' * 90}[/]")

                try:
                    ok, detail = await fn(client)
                except Exception as e:
                    ok, detail = False, f"EXCEPTION: {e}"

                scenario_results.append((name, ok, detail))
                status_icon = "✅" if ok else "❌"
                status_style = "green" if ok else "red"
                console.print(f"  [{status_style}]{status_icon} {detail}[/]")
                if not ok:
                    all_pass = False

        # ── Таблица результатов ──
        console.print(f"\n[bold cyan]{'═' * 90}[/]")
        results_table = Table(
            title="Результаты сценариев",
            show_header=True,
            header_style="bold magenta",
        )
        results_table.add_column("#", width=3)
        results_table.add_column("Сценарий", width=50)
        results_table.add_column("Статус", width=8)
        results_table.add_column("Детали", width=45)

        for i, (name, ok, detail) in enumerate(scenario_results, 1):
            status = "[green]PASS ✅[/]" if ok else "[red]FAIL ❌[/]"
            short = detail[:85] + "…" if len(detail) > 85 else detail
            results_table.add_row(str(i), name, status, short)

        console.print(results_table)

        # ── Сравнительная таблица ──
        console.print(f"\n[bold cyan]{'═' * 90}[/]")
        console.print(
            Panel("[bold]Нагрузочное сравнение: 50 запросов при истечении TTL[/]",
                  border_style="bright_yellow")
        )
        comparison = await run_comparison()

        cmp_table = Table(
            title="Без XFetch vs С XFetch (50 запросов, TTL=3s, delay=0.1s, β=2.0)",
            show_header=True,
            header_style="bold yellow",
        )
        cmp_table.add_column("Метрика", width=25)
        cmp_table.add_column("Без XFetch", width=25, justify="center")
        cmp_table.add_column("С XFetch", width=25, justify="center")
        cmp_table.add_column("Разница", width=20, justify="center")

        no_xf = comparison["Без XFetch"]
        with_xf = comparison["С XFetch"]

        # DB reads
        dr_diff = no_xf["db_reads"] - with_xf["db_reads"]
        dr_pct = (dr_diff / max(no_xf["db_reads"], 1)) * 100 if no_xf["db_reads"] > 0 else 0
        cmp_table.add_row(
            "DB reads",
            str(no_xf["db_reads"]),
            str(with_xf["db_reads"]),
            f"[green]-{dr_pct:.0f}%[/]" if dr_pct > 0 else "≈",
        )

        # Cache hits
        cmp_table.add_row(
            "Cache hits",
            str(no_xf["cache_hits"]),
            str(with_xf["cache_hits"]),
            "—",
        )

        # Recomputed (xfetch)
        cmp_table.add_row(
            "XFetch recomputes",
            str(no_xf["recomputed"]),
            str(with_xf["recomputed"]),
            "—",
        )

        # DB misses (full miss after TTL expiry)
        cmp_table.add_row(
            "DB misses (TTL expired)",
            str(no_xf["db_misses"]),
            str(with_xf["db_misses"]),
            "—",
        )

        # Time
        cmp_table.add_row(
            "Время (сек)",
            f"{no_xf['time']:.3f}",
            f"{with_xf['time']:.3f}",
            "—",
        )

        console.print(cmp_table)

        # ── Вывод ──
        console.print()
        key_finding = (
            f"[bold green]🎯 Ключевой результат:[/] "
            f"Без XFetch: {no_xf['db_misses']} full misses после TTL expiry (stampede). "
            f"С XFetch: кеш обновлялся {with_xf['recomputed']} раз ДО истечения TTL."
        )
        console.print(Panel(key_finding, border_style="green"))

        # ── Формула XFetch ──
        console.print(f"\n[bold cyan]{'═' * 90}[/]")
        console.print(
            Panel(
                "[bold]Формула XFetch (Probabilistic Early Recomputation)[/]\n\n"
                "  should_recompute = [bold yellow]now[/] > [bold blue]expiry[/] - "
                "[bold red]delta[/] × [bold magenta]β[/] × (-ln([bold green]random()[/]))\n\n"
                "  [bold yellow]now[/]      — текущее время (unix timestamp)\n"
                "  [bold blue]expiry[/]   — когда ключ истечёт (unix timestamp)\n"
                "  [bold red]delta[/]    — время последнего DB read (секунды)\n"
                "  [bold magenta]β (beta)[/]  — агрессивность (1.0 = стандарт)\n"
                "  [bold green]random()[/] — случайное число (0, 1]\n\n"
                "  -ln(random()) ≈ экспоненциальное распределение (среднее = 1)\n"
                "  Чем ближе now к expiry → чаще recompute = True\n"
                "  Чем больше delta → раньше начинается пересчёт\n"
                "  Чем больше β → агрессивнее пересчёт",
                border_style="bright_cyan",
            )
        )

        # ── Аналитический отчёт ──
        console.print(f"\n[bold cyan]{'═' * 90}[/]")
        console.print(
            Panel(
                "[bold]Аналитический отчёт — 8 вопросов[/]",
                border_style="bright_blue",
            )
        )

        for q, a in REPORT_QUESTIONS:
            console.print(f"\n[bold yellow]{q}[/]")
            console.print(f"[white]{a}[/]")

        # ── Финальный итог ──
        console.print(f"\n[bold cyan]{'═' * 90}[/]")
        passed = sum(1 for _, ok, _ in scenario_results if ok)
        total = len(scenario_results)

        if all_pass:
            console.print(
                Panel(
                    f"[bold green]✅ ВСЕ СЦЕНАРИИ ПРОЙДЕНЫ: {passed}/{total} PASS[/]\n\n"
                    "[dim]Проект Early Probabilistic Expiration (XFetch) завершён![/]",
                    border_style="green",
                    padding=(1, 4),
                )
            )
        else:
            console.print(
                Panel(
                    f"[bold red]⚠ ЕСТЬ ОШИБКИ: {passed}/{total} PASS[/]",
                    border_style="red",
                    padding=(1, 4),
                )
            )

        sys.exit(0 if all_pass else 1)

    finally:
        server_proc.terminate()
        try:
            server_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server_proc.kill()


if __name__ == "__main__":
    asyncio.run(main())
