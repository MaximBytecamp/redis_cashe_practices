from __future__ import annotations

import argparse
import asyncio
import statistics
import sys
import time

import httpx
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

console = Console()

BASE_URL = "http://127.0.0.1:8000"

async def api_call(client: httpx.AsyncClient, method: str, path: str, **kw) -> httpx.Response:
    if method == "GET":
        return await client.get(f"{BASE_URL}{path}", **kw)
    return await client.post(f"{BASE_URL}{path}", **kw)



async def warmup_cache(client: httpx.AsyncClient, product_ids: list[int], label: str):
    """Прогреть кэш — один запрос на каждый ID."""
    console.print(f"\n  Прогрев кэша ({label})…", style="yellow")

    tasks = [api_call(client, "GET", f"/product/{pid}") for pid in product_ids]
    await asyncio.gather(*tasks)

    #producuct_ids = [1,..., 50] -> pid=1 -> GET /product/1 -> [{api_call(GET, /product/1)}, {api_call(GET, /product/2)}, {3}, ... {50}]

    #gather([{api_call(GET, /product/1)}, {api_call(GET, /product/2)}, {3}, ... {50}] -> Task({1}), Task({2}) -> 

    console.print(f"  Прогрев завершён: {len(product_ids)} ключей\n", style="green")


async def send_burst(
    client: httpx.AsyncClient,
    product_ids: list[int],
    n_requests: int,
    concurrency: int = 50,
) -> list[float]:
    """
    Отправить n_requests запросов к случайным product_ids.
    Возвращает список latency (секунды).
    """
    import random

    sem = asyncio.Semaphore(concurrency)
    latencies: list[float] = []

    async def _one():
        pid = random.choice(product_ids)
        async with sem:
            t0 = time.perf_counter()
            await client.get(f"{BASE_URL}/product/{pid}")
            elapsed = time.perf_counter() - t0
            latencies.append(elapsed)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        console=console,
    ) as progress:
        task = progress.add_task("Запросы…", total=n_requests)
        batch_size = concurrency
        for start in range(0, n_requests, batch_size):
            end = min(start + batch_size, n_requests)

            batch = [_one() for _ in range(end - start)]
            await asyncio.gather(*batch)
            progress.update(task, advance=end - start)

    return latencies


def print_latency_stats(latencies: list[float], label: str):
    table = Table(title=f"Latency — {label}", show_lines=True)
    table.add_column("Metric", style="cyan", width=20)
    table.add_column("Value", style="bold white", justify="right")

    avg = statistics.mean(latencies) * 1000
    p50 = statistics.median(latencies) * 1000
    p95 = sorted(latencies)[int(len(latencies) * 0.95)] * 1000
    p99 = sorted(latencies)[int(len(latencies) * 0.99)] * 1000
    mn = min(latencies) * 1000
    mx = max(latencies) * 1000

    table.add_row("Requests", str(len(latencies)))
    table.add_row("Avg", f"{avg:.2f} ms")
    table.add_row("p50", f"{p50:.2f} ms")
    table.add_row("p95", f"{p95:.2f} ms")
    table.add_row("p99", f"{p99:.2f} ms")
    table.add_row("Min", f"{mn:.2f} ms")
    table.add_row("Max", f"{mx:.2f} ms")
    console.print(table)


async def get_server_metrics(client: httpx.AsyncClient) -> dict:
    resp = await api_call(client, "GET", "/metrics")
    return resp.json()


def print_server_metrics(data: dict, label: str):
    cache = data["cache"]
    db = data["db"]

    table = Table(title=f"Серверные метрики — {label}", show_lines=True)
    table.add_column("Metric", style="cyan", width=24)
    table.add_column("Value", style="bold white", justify="right")

    table.add_row("Cache Hits", str(cache["cache_hits"]))
    table.add_row("Cache Misses", str(cache["cache_misses"]))
    table.add_row("DB Hits", str(cache["db_hits"]))
    table.add_row("Hit Rate", f"{cache['hit_rate_%']}%")
    table.add_row("Null Cache Hits", str(cache["null_cache_hits"]))
    table.add_row("Lock Waits", str(cache["lock_waits"]))
    table.add_row("Local Cache Hits", str(cache["local_cache_hits"]))
    table.add_row("Server Avg Latency", f"{cache['avg_latency_ms']:.2f} ms")
    table.add_row("Server p95 Latency", f"{cache['p95_latency_ms']:.2f} ms")
    table.add_row("TTL Range", f"{cache['ttl_min']}–{cache['ttl_max']}")
    table.add_row("TTL Unique Values", str(cache["ttl_unique"]))
    table.add_row("DB Total Queries", str(db["total_queries"]))
    table.add_row("DB Avg Latency", f"{db['avg_db_latency_ms']:.2f} ms")
    console.print(table)


async def scenario_a(client: httpx.AsyncClient, n_requests: int, product_ids: list[int]):
    """Сценарий A — TTL без jitter."""
    console.print(Panel.fit(
        "[bold red]СЦЕНАРИЙ A — БЕЗ JITTER[/bold red]\n"
        "TTL = 60 (фиксированный)\n"
        "Mutex: OFF, Local Cache: OFF",
        border_style="red",
    ))

    await api_call(client, "POST", "/cache/flush")
    await api_call(client, "POST", "/metrics/reset")
    await api_call(client, "POST", "/cache/configure?use_jitter=false&use_mutex=false&use_local_cache=false&use_early_expiration=false")

    await warmup_cache(client, product_ids, "Scenario A")

    console.print(" Имитация истечения TTL (сбрасываем кэш)…", style="yellow")
    await api_call(client, "POST", "/cache/flush")
    await api_call(client, "POST", "/metrics/reset")
    await asyncio.sleep(0.5)

    console.print(f"\n  Отправляем {n_requests} запросов (cache cold!)…\n", style="red bold")
    latencies = await send_burst(client, product_ids, n_requests)

    print_latency_stats(latencies, "Scenario A (no jitter)")
    m = await get_server_metrics(client)
    print_server_metrics(m, "Scenario A (no jitter)")
    return m, latencies



async def scenario_b(client: httpx.AsyncClient, n_requests: int, product_ids: list[int]):
    """Сценарий B — TTL с jitter + anti-stampede + local cache."""
    console.print(Panel.fit(
        "[bold green]СЦЕНАРИЙ B — С JITTER + ANTI-STAMPEDE[/bold green]\n"
        "TTL = category-based + jitter\n"
        "Mutex: ON, Local Cache: ON",
        border_style="green",
    ))

    await api_call(client, "POST", "/cache/flush")
    await api_call(client, "POST", "/metrics/reset")
    await api_call(client, "POST", "/cache/configure?use_jitter=true&use_mutex=true&use_local_cache=true&use_early_expiration=false")

    await warmup_cache(client, product_ids, "Scenario B")

    # console.print("  Имитация истечения TTL (сбрасываем кэш)…", style="yellow")
    # await api_call(client, "POST", "/cache/flush")
    # await api_call(client, "POST", "/metrics/reset")
    # await asyncio.sleep(0.5)

    console.print(f"\n  Отправляем {n_requests} запросов (cache cold!)…\n", style="green bold")
    latencies = await send_burst(client, product_ids, n_requests)

    print_latency_stats(latencies, "Scenario B (with jitter)")
    m = await get_server_metrics(client)
    print_server_metrics(m, "Scenario B (with jitter)")
    return m, latencies

def print_comparison(ma: dict, la: list[float], mb: dict, lb: list[float]):
    console.print("\n")
    table = Table(title="Сравнение сценариев A vs B", show_lines=True)
    table.add_column("Metric", style="cyan", width=24)
    table.add_column("A (no jitter)", style="red", justify="right", width=18)
    table.add_column("B (with jitter)", style="green", justify="right", width=18)
    table.add_column("Δ", style="bold yellow", justify="right", width=14)

    ca, cb = ma["cache"], mb["cache"]
    da, db_m = ma["db"], mb["db"]

    rows = [
        ("DB Hits", ca["db_hits"], cb["db_hits"]),
        ("Cache Hits", ca["cache_hits"], cb["cache_hits"]),
        ("Hit Rate %", ca["hit_rate_%"], cb["hit_rate_%"]),
        ("Lock Waits", ca["lock_waits"], cb["lock_waits"]),
        ("Local Cache Hits", ca["local_cache_hits"], cb["local_cache_hits"]),
        ("TTL Unique Values", ca["ttl_unique"], cb["ttl_unique"]),
        ("Avg Latency (ms)", round(statistics.mean(la) * 1000, 2), round(statistics.mean(lb) * 1000, 2)),
        ("p95 Latency (ms)", round(sorted(la)[int(len(la) * 0.95)] * 1000, 2), round(sorted(lb)[int(len(lb) * 0.95)] * 1000, 2)),
        ("DB Total Queries", da["total_queries"], db_m["total_queries"]),
    ]

    for name, va, vb in rows:
        if isinstance(va, float):
            delta = f"{vb - va:+.2f}"
        else:
            delta = f"{vb - va:+d}" if isinstance(va, int) else str(vb - va)
        table.add_row(name, str(va), str(vb), delta)

    console.print(table)

    db_reduction = 0
    if da["total_queries"] > 0:
        db_reduction = (1 - db_m["total_queries"] / da["total_queries"]) * 100

    console.print(Panel.fit(
        f"[bold]Снижение нагрузки на БД: {db_reduction:.1f}%[/bold]\n\n"
        "[bold cyan]Выводы:[/bold cyan]\n"
        "• Без jitter все ключи истекают одновременно → [red]Cache Avalanche[/red]\n"
        "• Без mutex множество запросов одновременно идут в БД → [red]Cache Stampede[/red]\n"
        "• С jitter TTL разбросаны → нагрузка на БД распределена равномерно\n"
        "• С mutex только 1 запрос идёт в БД, остальные ждут → меньше DB hits\n"
        "• Local cache снимает нагрузку даже с Redis для горячих ключей\n\n"
        "[bold green]Jitter + Mutex — обязательная практика в production![/bold green]",
        title="Анализ",
        border_style="bright_blue",
    ))

async def main():
    global BASE_URL

    parser = argparse.ArgumentParser(description="Redis TTL Jitter — Load Test")
    parser.add_argument("--scenario", choices=["A", "B", "AB"], default="AB",
                        help="Какой сценарий запускать (default: AB — оба)")
    parser.add_argument("--requests", type=int, default=500,
                        help="Количество запросов в каждом сценарии (default: 500)")
    parser.add_argument("--products", type=int, default=50,
                        help="Количество различных product_id (default: 50)")
    parser.add_argument("--base-url", type=str, default=BASE_URL,
                        help="Base URL FastAPI сервера")
    args = parser.parse_args()

    BASE_URL = args.base_url
    product_ids = list(range(1, args.products + 1))

    console.print(Panel.fit(
        "[bold bright_white] Redis TTL + Jitter — Нагрузочное тестирование[/bold bright_white]\n\n"
        f"  Запросов на сценарий: [bold]{args.requests}[/bold]\n"
        f"  Уникальных продуктов: [bold]{args.products}[/bold]\n"
        f"  Сервер:               [bold]{BASE_URL}[/bold]\n"
        f"  Сценарии:             [bold]{args.scenario}[/bold]",
        border_style="bright_magenta",
    ))

    async with httpx.AsyncClient(timeout=30.0) as client:
        # Health check
        try:
            resp = await api_call(client, "GET", "/health")
            if resp.status_code != 200:
                raise Exception(f"Health check failed: {resp.status_code}")
            console.print("\n  Сервер доступен\n", style="green bold")
        except Exception as e:
            console.print(f"\n Сервер недоступен: {e}", style="red bold")
            console.print("  Запустите сервер: uvicorn app.main:app --reload", style="yellow")
            sys.exit(1)

        ma = mb = None
        la = lb = None


        if "A" in args.scenario:
            ma, la = await scenario_a(client, args.requests, product_ids)

        if "B" in args.scenario:
            mb, lb = await scenario_b(client, args.requests, product_ids)

        if ma and mb and la and lb:
            print_comparison(ma, la, mb, lb)

if __name__ == "__main__":
    asyncio.run(main())