from __future__ import annotations 


from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


#Path(/Users/makarovmn/Public/redis_cashe_practices/redis_ttl_jitter/.env)
_ENV_FILE = Path(__file__).resolve().parent.parent / ".env"

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file = str(_ENV_FILE),
        env_file_encoding="utf-8"
    )

    redis_host: str
    redis_port: int
    redis_db: int

    base_ttl: int = 60          # секунд
    jitter_max: int = 20        # максимальное отклонение

    db_delay_min: float = 0.1   # мин. задержка эмуляции БД
    db_delay_max: float = 0.3   # макс. задержка эмуляции БД


    app_host: str = "0.0.0.0"
    app_port: int = 8000

    @property
    def redis_url(self) -> str:
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"
    

settings = Settings()
    
