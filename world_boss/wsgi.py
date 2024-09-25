import sentry_sdk
from fastapi import FastAPI

from world_boss.app.api import api
from world_boss.app.config import config
from world_boss.app.graphql import graphql_app


def create_app() -> FastAPI:
    sentry_sdk.init(
        dsn=config.sentry_dsn,
        enable_tracing=True,
        traces_sample_rate=config.sentry_sample_rate,
    )
    fast_api = FastAPI()
    fast_api.include_router(api)
    fast_api.include_router(graphql_app, prefix="/graphql")
    return fast_api


app = create_app()
