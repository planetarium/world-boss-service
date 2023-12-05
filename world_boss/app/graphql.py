import typing

import strawberry
from fastapi import Depends
from strawberry.fastapi import GraphQLRouter
from strawberry.types import Info

from world_boss.app.api import get_db
from world_boss.app.data_provider import data_provider_client
from world_boss.app.enums import NetworkType
from world_boss.app.kms import MINER_URLS, signer
from world_boss.app.raid import get_currencies, get_next_tx_nonce


async def get_context(db=Depends(get_db)):
    return {
        "db": db,
    }


@strawberry.type
class Query:
    @strawberry.field
    def next_tx_nonce(self, info: Info) -> int:
        nonce = get_next_tx_nonce(info.context["db"])
        print(nonce)
        return nonce

    @strawberry.field
    def count_total_users(self, season_id: int) -> int:
        return data_provider_client.get_total_users_count(season_id, NetworkType.MAIN)

    @strawberry.field
    def check_balance(self, info: Info) -> typing.List[str]:
        currencies = get_currencies(info.context["db"])
        url = MINER_URLS[NetworkType.MAIN]
        result = []
        for currency in currencies:
            result.append(signer.query_balance(url, currency))
        return result


schema = strawberry.Schema(Query)
graphql_app = GraphQLRouter(schema, context_getter=get_context)
