import csv
from datetime import datetime
from tempfile import NamedTemporaryFile
from typing import List

from celery import Celery

from world_boss.app.data_provider import data_provider_client
from world_boss.app.enums import NetworkType
from world_boss.app.kms import signer, MINER_URLS
from world_boss.app.raid import update_agent_address, write_ranking_rewards_csv
from world_boss.app.slack import client
from world_boss.app.stubs import (
    RewardDictionary,
    CurrencyDictionary,
    RankingRewardWithAgentDictionary,
    RaiderWithAgentDictionary,
    Recipient,
)

celery = Celery()


@celery.task()
def count_users(channel_id: str, raid_id: int):
    total_count = data_provider_client.get_total_users_count(raid_id, NetworkType.MAIN)
    client.chat_postMessage(
        channel=channel_id,
        text=f"world boss season {raid_id} total users: {total_count}",
    )


@celery.task()
def get_ranking_rewards(
    channel_id: str, raid_id: int, total_count: int, start_nonce: int
):
    offset = 0
    size = 100
    results: List[RankingRewardWithAgentDictionary] = []
    while len(results) < total_count:
        result = data_provider_client.get_ranking_rewards(
            raid_id, NetworkType.MAIN, offset, size
        )
        rewards = update_agent_address(result, raid_id, NetworkType.MAIN, offset, size)
        results.extend(reward for reward in rewards if reward not in results)
        offset = len(results)
        size = min(size, total_count - offset)
    with NamedTemporaryFile(suffix=".csv") as temp_file:
        file_name = temp_file.name
        write_ranking_rewards_csv(file_name, results, raid_id, start_nonce)
        result_format = f"world_boss_{raid_id}_{total_count}_{start_nonce}_result"
        client.files_upload_v2(
            channels=channel_id,
            title=result_format,
            filename=f"{result_format}.csv",
            file=file_name,
        )


@celery.task()
def sign_transfer_assets(recipient_map: dict[int, List[Recipient]], time_string: str):
    # app context for task.
    from world_boss.wsgi import app

    with app.app_context():
        time_stamp = datetime.fromisoformat(time_string)
        for nonce in recipient_map:
            signer.transfer_assets(
                time_stamp,
                int(nonce),
                recipient_map[nonce],
                "world boss ranking rewards by world boss signer",
                MINER_URLS[NetworkType.MAIN],
            )
