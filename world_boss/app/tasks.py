from datetime import datetime
from tempfile import NamedTemporaryFile
from typing import List

from celery import Celery, chord

from world_boss.app.data_provider import data_provider_client
from world_boss.app.enums import NetworkType
from world_boss.app.kms import MINER_URLS, signer
from world_boss.app.models import Transaction, WorldBossReward, WorldBossRewardAmount
from world_boss.app.orm import db
from world_boss.app.raid import (
    get_next_tx_nonce,
    row_to_recipient,
    update_agent_address,
    write_ranking_rewards_csv,
)
from world_boss.app.slack import client
from world_boss.app.stubs import (
    RankingRewardWithAgentDictionary,
    Recipient,
    RecipientRow,
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
def prepare_world_boss_ranking_rewards(rows: List[RecipientRow], time_string: str):
    # app context for task.
    from world_boss.wsgi import app

    with app.app_context():
        # nonce : recipients for transfer_assets tx
        recipient_map: dict[int, list[Recipient]] = {}
        max_nonce = get_next_tx_nonce() - 1
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
        chord(
            sign_transfer_assets.s(
                time_string, int(nonce), recipient_map[nonce], memo, url, max_nonce
            )
            for nonce in recipient_map
        )(insert_world_boss_rewards.si(rows))


@celery.task()
def sign_transfer_assets(
    time_string: str,
    nonce: int,
    recipients: List[Recipient],
    memo: str,
    url: str,
    max_nonce: int,
):
    # app context for task.
    from world_boss.wsgi import app

    with app.app_context():
        if nonce > max_nonce:
            time_stamp = datetime.fromisoformat(time_string)
            signer.transfer_assets(time_stamp, nonce, recipients, memo, url)


@celery.task()
def insert_world_boss_rewards(rows: List[RecipientRow]):
    # app context for task.
    from world_boss.wsgi import app

    with app.app_context():
        # ranking : world_boss_reward
        world_boss_rewards: dict[int, WorldBossReward] = {}
        with db.session.no_autoflush:  # type: ignore
            transactions = db.session.query(Transaction).filter_by(
                signer="0xCFCd6565287314FF70e4C4CF309dB701C43eA5bD"
            )
            # raid_id,ranking,agent_address,avatar_address,amount,ticker,decimal_places,target_nonce
            for row in rows:
                # parse row
                raid_id = int(row[0])
                ranking = int(row[1])
                agent_address = row[2]
                avatar_address = row[3]
                amount = int(row[4])
                ticker = row[5]
                decimal_places = int(row[6])
                nonce = int(row[7])

                # get or create world_boss_reward
                if world_boss_rewards.get(ranking):
                    world_boss_reward = world_boss_rewards[ranking]
                else:
                    world_boss_reward = WorldBossReward()
                    world_boss_reward.raid_id = raid_id
                    world_boss_reward.ranking = ranking
                    world_boss_reward.agent_address = agent_address
                    world_boss_reward.avatar_address = avatar_address
                    world_boss_rewards[ranking] = world_boss_reward

                # create world_boss_reward_amount
                world_boss_reward_amount = WorldBossRewardAmount()
                world_boss_reward_amount.amount = amount
                world_boss_reward_amount.decimal_places = decimal_places
                world_boss_reward_amount.ticker = ticker
                world_boss_reward_amount.reward = world_boss_reward
                world_boss_reward_amount.transaction = transactions.filter_by(
                    nonce=nonce
                ).one()
            db.session.commit()
