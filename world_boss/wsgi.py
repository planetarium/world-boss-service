import os

from flask import Flask
from sqlalchemy.exc import OperationalError

from world_boss.app.api import api
from world_boss.app.orm import db, migrate
from world_boss.app.tasks import celery


def create_app(
    db_uri: str = "postgresql://postgres@localhost:5432/world-boss",
) -> Flask:
    flask_app = Flask(__name__)
    flask_app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL", db_uri)
    flask_app.register_blueprint(api)

    # Celery
    default_redis_url = "redis://localhost:6379"
    flask_app.config.update(
        CELERY_BROKER_URL=os.environ.get("CELERY_BROKER_URL", f"{default_redis_url}/0"),
        CELERY_RESULT_BACKEND=os.environ.get(
            "CELERY_RESULT_BACKEND", f"{default_redis_url}/1"
        ),
    )

    db.init_app(flask_app)
    migrate.init_app(flask_app, db)
    return flask_app


def make_celery(flask_app):
    celery.conf.update(
        result_backend=flask_app.config["CELERY_RESULT_BACKEND"],
        broker_url=flask_app.config["CELERY_BROKER_URL"],
    )
    celery.conf.update(flask_app.config)
    celery.conf.timezone = "UTC"

    class BaseTaskWithRetry(celery.Task):
        autoretry_for = (OperationalError,)
        max_retries = 3
        retry_backoff = True
        retry_backoff_max = 180
        retry_jitter = True

    celery.Task = BaseTaskWithRetry
    return celery


def index():
    return "hello world"


app = create_app()
cel = make_celery(app)
