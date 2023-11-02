import sentry_sdk
from flask import Flask
from sqlalchemy.exc import OperationalError

from world_boss.app.api import api
from world_boss.app.config import config
from world_boss.app.orm import db, migrate
from world_boss.app.tasks import celery


def create_app() -> Flask:
    sentry_sdk.init(
        dsn=config.sentry_dsn,
        enable_tracing=True,
        traces_sample_rate=config.sentry_sample_rate,
    )
    flask_app = Flask(__name__)
    flask_app.config["SQLALCHEMY_DATABASE_URI"] = config.database_url
    flask_app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    flask_app.register_blueprint(api)

    # Celery
    flask_app.config.update(
        CELERY_BROKER_URL=config.celery_broker_url,
        CELERY_RESULT_BACKEND=config.celery_result_backend,
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

    class FlaskTaskWithRetry(celery.Task):
        autoretry_for = (OperationalError,)
        max_retries = 3
        retry_backoff = True
        retry_backoff_max = 180
        retry_jitter = True

        def __call__(self, *args, **kwargs):
            with flask_app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = FlaskTaskWithRetry
    return celery


app = create_app()
cel = make_celery(app)
