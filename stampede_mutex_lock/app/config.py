from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV_FILE = Path(__file__).resolve().parent.parent / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(_ENV_FILE), env_file_encoding="utf-8"
    )

    # ── Redis
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 4

    # ── Database 
    database_url: str = "sqlite+aiosqlite:///./products_sm.db"

    # ── Сервер
    app_host: str = "0.0.0.0"
    app_port: int = 8040

    # ── TTL кэша товара (сек)
    product_cache_ttl: int = 120

    # ── Mutex Lock параметры
    lock_ttl_seconds: int = 5          # TTL lock-ключа (сек)
    lock_retry_delay_ms: int = 80      # Пауза между retry (мс)
    lock_max_retries: int = 5          # Макс. попыток retry

    # ── Имитация медленной БД (сек)
    db_simulate_delay: float = 0.0

    # ── Включение/выключение stampede-защиты
    stampede_protection_enabled: bool = True


settings = Settings()
