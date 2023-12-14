import csv
import typing
from io import StringIO

import httpx
import strawberry
from celery import chord
from fastapi import Depends
from strawberry import BasePermission
from strawberry.fastapi import GraphQLRouter
from strawberry.types import Info

from world_boss.app.api import get_db
from world_boss.app.config import config
from world_boss.app.data_provider import data_provider_client
from world_boss.app.enums import NetworkType
from world_boss.app.kms import HEADLESS_URLS, MINER_URLS, signer
from world_boss.app.models import Transaction
from world_boss.app.raid import (
    get_currencies,
    get_next_tx_nonce,
    list_tx_nonce,
    row_to_recipient,
)
from world_boss.app.slack import client
from world_boss.app.stubs import Recipient
from world_boss.app.tasks import (
    get_ranking_rewards,
    insert_world_boss_rewards,
    query_tx_result,
    send_slack_message,
    sign_transfer_assets,
    stage_transaction,
    upload_prepare_reward_assets,
    upload_tx_result,
)


async def get_context(db=Depends(get_db)):
    return {
        "db": db,
    }


class IsAuthenticated(BasePermission):
    message = "User is not authenticated"

    # This method can also be async!
    def has_permission(self, source: typing.Any, info: Info, **kwargs) -> bool:
        return kwargs["password"] == config.graphql_password


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


@strawberry.type
class Mutation:
    @strawberry.mutation
    def generate_ranking_rewards_csv(
        self,
        season_id: int,
        total_users: int,
        start_nonce: int,
        password: str,
    ) -> str:
        task = get_ranking_rewards.delay(
            config.slack_channel_id, season_id, total_users, start_nonce
        )
        return task.id

    @strawberry.mutation
    def prepare_transfer_assets(
        self, link: str, time_stamp: str, password: str, info: Info
    ) -> str:
        db = info.context["db"]
        file_id = link.split("/")[5]
        res = client.files_info(file=file_id)
        data = typing.cast(dict, res.data)
        file = data["file"]
        content = httpx.get(
            file["url_private"], headers={"Authorization": "Bearer %s" % client.token}
        ).content.decode()
        stream = StringIO(content)
        has_header = csv.Sniffer().has_header(content)
        reader = csv.reader(stream)
        if has_header:
            next(reader, None)
        # nonce : recipients for transfer_assets tx
        recipient_map: dict[int, list[Recipient]] = {}
        max_nonce = get_next_tx_nonce(db) - 1
        exist_nonce = list_tx_nonce(db)
        rows = [row for row in reader]
        # raid_id,ranking,agent_address,avatar_address,amount,ticker,decimal_places,target_nonce
        for row in rows:
            nonce = int(row[7])
            recipient = row_to_recipient(row)

            # update recipient_map
            if not recipient_map.get(nonce):
                recipient_map[nonce] = []
            recipient_map[nonce].append(recipient)

        # sanity check
        for k in recipient_map:
            assert len(recipient_map[k]) <= 100
        # insert tables
        memo = "world boss ranking rewards by world boss signer"
        url = MINER_URLS[NetworkType.MAIN]
        task = chord(
            sign_transfer_assets.s(
                time_stamp,
                int(nonce),
                recipient_map[nonce],
                memo,
                url,
                max_nonce,
                exist_nonce,
            )
            for nonce in recipient_map
        )(insert_world_boss_rewards.si(rows))
        return task.id

    @strawberry.mutation
    def prepare_reward_assets(self, season_id: int, password: str) -> str:
        task = upload_prepare_reward_assets.delay(config.slack_channel_id, season_id)
        return task.id

    @strawberry.mutation
    def stage_transactions(self, password: str, info: Info) -> str:
        db = info.context["db"]
        nonce_list = (
            db.query(Transaction.nonce)
            .filter_by(signer=signer.address, tx_result=None)
            .all()
        )
        network_type = NetworkType.MAIN
        headless_urls = HEADLESS_URLS[network_type]
        task = chord(
            stage_transaction.s(headless_url, nonce)
            for headless_url in headless_urls
            for nonce, in nonce_list
        )(
            send_slack_message.si(
                config.slack_channel_id, f"stage {len(nonce_list)} transactions"
            )
        )
        return task.id

    @strawberry.mutation(permission_classes=[IsAuthenticated])
    def transaction_result(self, password: str, info: Info) -> str:
        db = info.context["db"]
        tx_ids = db.query(Transaction.tx_id).filter_by(tx_result=None)
        url = MINER_URLS[NetworkType.MAIN]
        task = chord(query_tx_result.s(url, str(tx_id)) for tx_id, in tx_ids)(
            upload_tx_result.s(config.slack_channel_id)
        )
        return task.id


schema = strawberry.Schema(Query, mutation=Mutation)
graphql_app = GraphQLRouter(schema, context_getter=get_context)
