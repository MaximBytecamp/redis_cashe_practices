from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV_FILE = Path(__file__).resolve().parent.parent / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=str(_ENV_FILE), env_file_encoding="utf-8")

    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 6

    database_url: str = "sqlite+aiosqlite:///./products_bf.db"

    product_ttl: int = 300

    negative_ttl: int = 60

    app_host: str = "0.0.0.0"
    app_port: int = 8060

    # Bloom filter параметры
    bloom_expected_items: int = 10_000
    bloom_fp_rate: float = 0.01      # 1% ложных срабатываний
    bloom_key: str = "bloom:products"


    bloom_enabled: bool = True
    negative_cache_enabled: bool = True


settings = Settings()
