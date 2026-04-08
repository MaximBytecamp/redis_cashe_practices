from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV_FILE = Path(__file__).resolve().parent.parent / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=str(_ENV_FILE), env_file_encoding="utf-8")

    redis_host: str 
    redis_port: int
    redis_db: int

    database_url: str

    base_ttl: int 
    jitter_max: int 

    app_host: str = "0.0.0.0"
    app_port: int = 8020

    # ── Режимы для демо-переключения
    write_through_enabled: bool = True    # write-through для карточки
    invalidate_lists: bool = True         # invalidate для списков/stats
    sync_mode: str = "hybrid"             # "write_through" | "invalidate" | "hybrid" | "none"

    @property
    def redis_url(self) -> str:
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"


settings = Settings()