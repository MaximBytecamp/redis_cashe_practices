from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV_FILE = Path(__file__).resolve().parent.parent / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=str(_ENV_FILE), env_file_encoding="utf-8")

    redis_host: str = "localhost"
    redis_port: int
    redis_db: int = 3

    database_url: str

    product_ttl: int

    null_ttl: int

    app_host: str = "0.0.0.0"
    app_port: int = 8030

    # Включение/выключение null caching (для сравнительного тестирования)
    null_caching_enabled: bool = True


settings = Settings()
