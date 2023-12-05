import strawberry
from fastapi import Depends
from strawberry.fastapi import GraphQLRouter
from strawberry.types import Info

from world_boss.app.api import get_db
from world_boss.app.raid import get_next_tx_nonce


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


schema = strawberry.Schema(Query)
graphql_app = GraphQLRouter(schema, context_getter=get_context)
