from typing import TYPE_CHECKING

from pydantic import BaseSettings

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
    slack_signing_secret: str
    sentry_dsn: str = ""
    sentry_sample_rate: float = 0.1
    slack_channel_id: str
    graphql_password: str
    headless_url: str
    data_provider_url: str
    headless_jwt_secret: str
    headless_jwt_iss: str
    headless_jwt_algorithm: str
    planet_id: str
    scheduler_interval: int = 60 * 5

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
            "slack_signing_secret": {"env": "SLACK_SIGNING_SECRET"},
            "sentry_dsn": {
                "env": "SENTRY_DSN",
            },
            "sentry_sample_rate": {"env": "SENTRY_SAMPLE_RATE"},
            "slack_channel_id": {"env": "SLACK_CHANNEL_ID"},
            "graphql_password": {"env": "GRAPHQL_PASSWORD"},
            "headless_url": {"env": "HEADLESS_URL"},
            "data_provider_url": {"env": "DATA_PROVIDER_URL"},
            "headless_jwt_secret": {"env": "HEADLESS_JWT_SECRET"},
            "headless_jwt_iss": {"env": "HEADLESS_JWT_ISS"},
            "headless_jwt_algorithm": {"env": "HEADLESS_JWT_ALGORITHM"},
            "planet_id": {"env": "PLANET_ID"},
            "scheduler_interval": {"env": "SCHEDULER_INTERVAL"},
        }


config = Settings()
