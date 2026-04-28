"""Конфигурация проекта — Early Probabilistic Expiration (XFetch)."""

from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV_FILE = Path(__file__).resolve().parent.parent / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(_ENV_FILE), env_file_encoding="utf-8"
    )

    # ── Redis ──
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 5

    # ── Database ──
    database_url: str = "sqlite+aiosqlite:///./products_xfetch.db"

    # ── Сервер ──
    app_host: str = "0.0.0.0"
    app_port: int = 8050

    # ── TTL кэша товара (сек) ──
    product_cache_ttl: int = 120

    # ── XFetch параметры ──
    xfetch_beta: float = 1.0            # коэффициент β (больше = более ранний пересчёт)
    xfetch_enabled: bool = True          # вкл/выкл XFetch (для сравнения)

    # ── Имитация медленной БД (сек) ──
    db_simulate_delay: float = 0.0


settings = Settings()
