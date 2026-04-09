#!/usr/bin/env python3
"""
run_demo.py — Единый запуск Write-Through After Update.

  1. Архитектурная диаграмма
  2. Запуск FastAPI-сервера (background)
  3. Прогон всех сценариев
  4. Остановка сервера
  5. Аналитический отчёт
"""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import signal
import sys
import time

import httpx
import uvicorn
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.table import Table

console = Console(width=110)

BASE_URL = "http://127.0.0.1:8020"
PORT = 8020

# ── Architecture Diagram ────────────────────────────────────

ARCHITECTURE = """
```
┌─────────────────────────────────────────────────────────────────────┐
│                    Write-Through After Update                       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────┐   READ (Cache-Aside)                                  │
│  │  Client   │─────────────────────────┐                            │
│  │ (httpx)   │                         ▼                            │
│  └──────┬────┘                  ┌─────────────┐   MISS   ┌───────┐ │
│         │                       │    Redis     │────────▶ │ SQLite│ │
│         │                       │  (DB=2)      │◀────────│  (DB) │ │
│         │                       └──────────────┘  SET     └───────┘ │
│         │                              ▲                     ▲      │
│         │   WRITE                      │                     │      │
│         └──────────┐                   │                     │      │
│                    ▼                   │                     │      │
│            ┌──────────────┐            │                     │      │
│            │ ProductService│────────────┘                     │      │
│            └──────┬───────┘     CacheSyncService              │      │
│                   │                                          │      │
│                   └──────────── DB WRITE ─────────────────────┘      │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │                    CacheSyncService                         │    │
│  ├─────────────────────────────────────────────────────────────┤    │
│  │  mode=write_through  │ product:{id} → WT                   │    │
│  │                      │ products:all → WT                    │    │
│  │                      │ products:category:* → WT             │    │
│  │                      │ stats:products → WT                  │    │
│  ├──────────────────────┼──────────────────────────────────────┤    │
│  │  mode=invalidate     │ product:{id} → DELETE                │    │
│  │                      │ products:all → DELETE                 │    │
│  │                      │ products:category:* → DELETE          │    │
│  │                      │ stats:products → DELETE               │    │
│  ├──────────────────────┼──────────────────────────────────────┤    │
│  │  mode=hybrid         │ product:{id} → WT (мгновенно)       │    │
│  │   (рекомендуемый)    │ products:all → DELETE (лениво)       │    │
│  │                      │ products:category:* → DELETE          │    │
│  │                      │ stats:products → DELETE               │    │
│  ├──────────────────────┼──────────────────────────────────────┤    │
│  │  mode=none           │ (ничего — stale cache!)              │    │
│  └──────────────────────┴──────────────────────────────────────┘    │
│                                                                     │
│  Ключи Redis:                                                       │
│    product:{id}         — JSON карточка товара                      │
│    products:all         — JSON список всех товаров                  │
│    products:category:X  — JSON список по категории                  │
│    stats:products       — JSON агрегированная статистика            │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```
"""

# ── Server Management ───────────────────────────────────────


def _run_server():
    """Запуск uvicorn в отдельном процессе."""
    uvicorn.run(
        "app.main:app",
        host="127.0.0.1",
        port=PORT,
        log_level="warning",
    )


async def wait_for_server(timeout: float = 15.0) -> bool:
    """Ждём готовности сервера."""
    deadline = time.time() + timeout
    async with httpx.AsyncClient() as client:
        while time.time() < deadline:
            try:
                r = await client.get(f"{BASE_URL}/health", timeout=2.0)
                if r.status_code == 200:
                    return True
            except Exception:
                pass
            await asyncio.sleep(0.3)
    return False


# ── Report ──────────────────────────────────────────────────

REPORT_TEXT = """
## Аналитический отчёт: Write-Through After Update

### 1. Что такое Write-Through?
При **write-through** после записи в БД данные **немедленно записываются** в кеш.
В отличие от invalidate (удаление), write-through **гарантирует cache hit** на следующем чтении.

### 2. Когда использовать Write-Through?
- **Горячие ключи** с высокой частотой чтения (карточки товаров, профили)
- Когда **cache miss дорогой** (сложная агрегация, JOIN-запросы)
- Когда **свежесть данных критична** (цены, остатки)

### 3. Когда НЕ использовать Write-Through?
- Для **производных представлений** (списки, статистика) — дорого пересчитывать при каждом update
- Для **редко читаемых данных** — бесполезная нагрузка на Redis
- При **массовых обновлениях** — N записей в Redis вместо 1 DELETE

### 4. Почему гибридная стратегия оптимальна?
| Ключ | Стратегия | Причина |
|------|-----------|---------|
| `product:{id}` | Write-Through | Горячий ключ, частые чтения |
| `products:all` | Invalidate | Дешевле удалить, чем пересчитать при каждом update |
| `products:category:*` | Invalidate | Зависит от многих товаров |
| `stats:products` | Invalidate | Агрегация — ленивый пересчёт |

### 5. Write-Through vs Invalidate: Сравнение
| Критерий | Write-Through | Invalidate |
|----------|--------------|------------|
| Cache Hit после update | ✅ Гарантирован | ❌ Cache Miss |
| Latency записи | ⬆ Выше (доп. SET) | ⬇ Ниже (только DEL) |
| Консистентность | ✅ Мгновенная | ⚠ Eventual (до след. чтения) |
| Сложность | Средняя | Низкая |
| Нагрузка на Redis | ⬆ Больше (SET с данными) | ⬇ Меньше (DEL) |

### 6. Write-Through + TTL
Write-Through **не отменяет TTL**! TTL остаётся страховкой:
- Защита от утечки памяти
- Гарантия обновления если sync не сработал
- Дедлайн свежести для менее критичных данных

### 7. Результаты нагрузочного теста
В сценарии 7 мы увидели:
- **write_through**: 0 stale reads, 100% cache hits, но выше latency записи
- **invalidate**: 0 stale reads, 0% cache hits (каждое чтение — из БД)
- **hybrid**: 0 stale reads для карточки, оптимальный баланс
- **none**: stale reads неизбежны

**Вывод**: Гибридная стратегия — лучший production-подход для большинства систем.
"""


def print_report():
    console.print()
    console.print(Panel(
        Markdown(REPORT_TEXT),
        title="[bold yellow]📊 Аналитический отчёт[/]",
        border_style="yellow",
        padding=(1, 2),
    ))


# ── Main ────────────────────────────────────────────────────


async def main():
    global BASE_URL

    console.print(Panel(
        "[bold magenta]🚀 Write-Through After Update — Full Demo[/]\n"
        "Проект 3: Гибридная стратегия кеширования\n"
        "write-through для карточек + invalidate для списков",
        border_style="magenta",
    ))

    # 1. Architecture
    console.print(Panel(
        Markdown(ARCHITECTURE),
        title="[bold blue]Архитектура[/]",
        border_style="blue",
    ))

    # 2. Start server
    console.print("[bold cyan]▶ Запуск сервера на порту 8020…[/]")

    # Удаляем старую БД для чистого старта
    db_file = os.path.join(os.path.dirname(__file__), "products_wt.db")
    if os.path.exists(db_file):
        os.remove(db_file)

    server_proc = multiprocessing.Process(target=_run_server, daemon=True)
    server_proc.start()

    if not await wait_for_server():
        console.print("[red]❌ Сервер не стартовал![/]")
        server_proc.terminate()
        sys.exit(1)

    console.print("[green]✅ Сервер запущен[/]\n")

    # 3. Run scenarios
    try:
        from run_scenarios import run_all
        results = await run_all()
    except Exception as e:
        console.print(f"[red]Ошибка сценариев: {e}[/]")
        results = []
    finally:
        # 4. Stop server
        console.print("\n[cyan]■ Остановка сервера…[/]")
        server_proc.terminate()
        server_proc.join(timeout=5)
        if server_proc.is_alive():
            os.kill(server_proc.pid, signal.SIGKILL)

    # 5. Summary
    if results:
        total_pass = sum(1 for _, p in results if p)
        total = len(results)

        console.print()
        t = Table(title="[bold]Итоги тестирования[/]", border_style="green")
        t.add_column("Сценарий", style="bold")
        t.add_column("Результат", justify="center")
        for name, passed in results:
            mark = "[green]✅ PASS[/]" if passed else "[red]❌ FAIL[/]"
            t.add_row(name, mark)
        console.print(t)

        color = "green" if total_pass == total else "red"
        console.print(
            Panel(
                f"[bold {color}]{total_pass}/{total} сценариев пройдено[/]",
                border_style=color,
            )
        )

    # 6. Report
    print_report()

    console.print("[bold magenta]Demo complete! 🎉[/]\n")


if __name__ == "__main__":
    asyncio.run(main())
