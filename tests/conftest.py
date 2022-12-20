import os
import typing
from decimal import Decimal

import pytest
import sqlalchemy as sa
from flask import Flask
from flask.testing import FlaskClient
from pytest_postgresql.janitor import DatabaseJanitor
from pytest_redis import factories  # type: ignore

from world_boss.app.config import config
from world_boss.app.models import Transaction, WorldBossReward, WorldBossRewardAmount
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


@pytest.fixture()
def fx_world_boss_reward_amounts(fx_session) -> typing.List[WorldBossRewardAmount]:
    reward = WorldBossReward()
    reward.avatar_address = "avatar_address"
    reward.agent_address = "agent_address"
    reward.raid_id = 1
    reward.ranking = 1
    result = []
    fx_session.add(reward)
    i = 1
    for ticker, decimal_places in [("CRYSTAL", 18), ("RUNE_FENRIR1", 0)]:
        reward_amount = WorldBossRewardAmount()
        reward_amount.amount = Decimal("10")
        reward_amount.ticker = ticker
        reward_amount.decimal_places = decimal_places
        reward_amount.reward = reward
        tx_id = str(i)
        reward_amount.tx_id = tx_id
        transaction = Transaction()
        transaction.tx_id = tx_id
        transaction.signer = "signer"
        transaction.payload = f"10 {ticker}"
        transaction.nonce = i
        fx_session.add(transaction)
        result.append(reward_amount)
        i += 1
    fx_session.commit()
    return result


@pytest.fixture(scope="session")
def celery_config(fx_app: Flask):
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
