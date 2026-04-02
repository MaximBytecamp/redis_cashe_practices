"""
Конфигурация проекта — все настройки из .env.
"""

from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV_FILE = Path(__file__).resolve().parent.parent / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(_ENV_FILE),
        env_file_encoding="utf-8",
    )

    # ── Redis 
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 1

    # ── Database 
    database_url: str = "sqlite+aiosqlite:///./products.db"

    # ── TTL 
    base_ttl: int = 300
    jitter_max: int = 60

    # ── App 
    app_host: str = "0.0.0.0"
    app_port: int = 8010

    # ── Режим invalidate 
    invalidate_enabled: bool = True

    @property
    def redis_url(self) -> str:
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"


settings = Settings()
