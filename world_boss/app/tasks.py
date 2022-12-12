import csv
from tempfile import NamedTemporaryFile
from typing import List

from celery import Celery

from world_boss.app.data_provider import data_provider_client
from world_boss.app.enums import NetworkType
from world_boss.app.raid import update_agent_address, write_ranking_rewards_csv
from world_boss.app.slack import client
from world_boss.app.stubs import (
    RewardDictionary,
    CurrencyDictionary,
    RankingRewardWithAgentDictionary,
    RaiderWithAgentDictionary,
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
    limit = 100
    results: List[RankingRewardWithAgentDictionary] = []
    for i in range(offset, int(total_count / limit) + 1):
        result = data_provider_client.get_ranking_rewards(
            raid_id, NetworkType.MAIN, i * 100, limit
        )
        rewards = update_agent_address(
            result, raid_id, NetworkType.MAIN, i * 100, limit
        )
        for r in rewards:
            results.append(r)
    assert len(results) == total_count
    temp_file = NamedTemporaryFile(suffix=".csv")
    file_name = temp_file.name
    write_ranking_rewards_csv(file_name, results, raid_id, start_nonce)
    client.files_upload_v2(
        channels=channel_id, title="test", filename="test.csv", file=file_name
    )
