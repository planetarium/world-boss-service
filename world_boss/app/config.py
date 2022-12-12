from typing import TYPE_CHECKING
from pydantic import BaseSettings, Field

if TYPE_CHECKING:
    PostgresDsn = str
    RedisDsn = str
else:
    from pydantic import PostgresDsn, RedisDsn


__all__ = "config"


class Settings(BaseSettings):
    database_url: PostgresDsn
    default_redis_url: RedisDsn = "redis://localhost:6379"
    kms_key_id: str
    slack_token: str
    redis_host: str
    redis_port: int
    celery_broker_url: str = f"{default_redis_url}/0"
    celery_result_backend: str = f"{default_redis_url}/1"

    class Config:
        env_file = ".env"
        fields = {
            "database_url": {
                "env": "DATABASE_URL",
            },
            "kms_key_id": {
                "env": "KMS_KEY_ID",
            },
            "slack_token": {
                "env": "SLACK_TOKEN",
            },
            "redis_host": {
                "env": "REDIS_HOST",
            },
            "redis_port": {
                "env": "REDIS_PORT",
            },
            "celery_broker_url": {
                "env": "CELERY_BROKER_URL",
            },
            "celery_result_backend": {
                "env": "CELERY_RESULT_BACKEND",
            },
        }


config = Settings()
