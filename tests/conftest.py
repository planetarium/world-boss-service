import typing

import pytest
import sqlalchemy as sa
from flask import Flask
from flask.testing import FlaskClient
from pytest_postgresql.janitor import DatabaseJanitor
from pytest_redis import factories  # type: ignore

from world_boss.app.config import config
from world_boss.app.models import Transaction
from world_boss.app.stubs import RewardDictionary
from world_boss.wsgi import create_app

redis_proc = factories.redis_proc(port=6379)

DB_OPTS = sa.engine.url.make_url(config.database_url).translate_connect_args()


@pytest.fixture(scope="session")
def database():
    """
    Create a Postgres database for the tests, and drop it when the tests are done.
    """
    pg_host = DB_OPTS.get("host")
    pg_port = DB_OPTS.get("port")
    pg_user = DB_OPTS.get("username")
    pg_pass = DB_OPTS.get("password")
    pg_db = DB_OPTS["database"]

    janitor = DatabaseJanitor(pg_user, pg_host, pg_port, pg_db, 9.6, pg_pass)
    janitor.init()
    yield
    janitor.drop()


@pytest.fixture(scope="session")
def fx_app(database) -> Flask:
    fx_app = create_app()
    ctx = fx_app.app_context()
    ctx.push()
    return fx_app


@pytest.fixture
def fx_session(fx_app):
    """
    Provide the transactional fixtures with access to the database via a Flask-SQLAlchemy
    database connection.
    """
    fx_db = fx_app.extensions["sqlalchemy"].db
    fx_db.session.rollback()
    fx_db.drop_all()
    fx_db.session.commit()
    fx_db.create_all()
    return fx_db.session


@pytest.fixture()
def fx_test_client(fx_app: Flask) -> FlaskClient:
    fx_app.testing = True
    return fx_app.test_client()


@pytest.fixture(scope="session")
def celery_config(fx_app: Flask, redis_proc):
    conf = {
        "broker_url": fx_app.config["CELERY_BROKER_URL"],
        "result_backend": fx_app.config["CELERY_RESULT_BACKEND"],
    }
    conf.update(fx_app.config)
    return conf


@pytest.fixture()
def fx_ranking_rewards() -> typing.List[RewardDictionary]:
    return [
        {
            "currency": {
                "decimalPlaces": 18,
                "minters": None,
                "ticker": "CRYSTAL",
            },
            "quantity": "1000000",
        },
        {
            "currency": {
                "decimalPlaces": 0,
                "minters": None,
                "ticker": "RUNESTONE_FENRIR1",
            },
            "quantity": "3500",
        },
        {
            "currency": {
                "decimalPlaces": 0,
                "minters": None,
                "ticker": "RUNESTONE_FENRIR2",
            },
            "quantity": "1200",
        },
        {
            "currency": {
                "decimalPlaces": 0,
                "minters": None,
                "ticker": "RUNESTONE_FENRIR3",
            },
            "quantity": "300",
        },
    ]


@pytest.fixture()
def fx_transactions() -> typing.List[Transaction]:
    transactions = []
    for nonce, tx_id, payload in [
        (
            1,
            "a9c9444bd50b3164b5c251315960272ae1f42f7b2d5b95948a78c608424bbcb2",
            "payload_1",
        ),
        (
            2,
            "db4b916c5c821cbf90356694f231c9f6a6858b67231799dc9ee2d9f2946c4310",
            "payload_2",
        ),
    ]:
        transaction = Transaction()
        transaction.tx_id = tx_id
        transaction.nonce = nonce
        transaction.payload = payload
        transaction.signer = "0x2531e5e06cBD11aF54f98D39578990716fFC7dBa"
        transactions.append(transaction)
    return transactions
