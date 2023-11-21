from datetime import datetime
from tempfile import NamedTemporaryFile
from typing import List, Tuple

import bencodex
from celery import Celery
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from world_boss.app.config import config
from world_boss.app.data_provider import DATA_PROVIDER_URLS, data_provider_client
from world_boss.app.enums import NetworkType
from world_boss.app.kms import MINER_URLS, signer
from world_boss.app.models import Transaction, WorldBossReward, WorldBossRewardAmount
from world_boss.app.raid import (
    get_assets,
    update_agent_address,
    write_ranking_rewards_csv,
    write_tx_result_csv,
)
from world_boss.app.slack import client
from world_boss.app.stubs import (
    CurrencyDictionary,
    RankingRewardWithAgentDictionary,
    Recipient,
    RecipientRow,
)

celery = Celery()
celery.conf.broker_url = config.celery_broker_url
celery.conf.result_backend = config.celery_result_backend
celery.conf.timezone = "UTC"

task_engine = create_engine(str(config.database_url))
TaskSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=task_engine)


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
        try:
            result = data_provider_client.get_ranking_rewards(
                raid_id, NetworkType.MAIN, offset, size
            )
        except Exception as e:
            client.chat_postMessage(
                channel=channel_id,
                text=f"failed to get rewards from {DATA_PROVIDER_URLS[NetworkType.MAIN]} exc: {e}",
            )
            raise e
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
def sign_transfer_assets(
    time_string: str,
    nonce: int,
    recipients: List[Recipient],
    memo: str,
    url: str,
    max_nonce: int,
    exist_nonce: List[int],
):
    with TaskSessionLocal() as db:
        if nonce > max_nonce or nonce not in exist_nonce:
            time_stamp = datetime.fromisoformat(time_string)
            signer.transfer_assets(time_stamp, nonce, recipients, memo, url, db)


@celery.task()
def insert_world_boss_rewards(rows: List[RecipientRow]):
    # ranking : world_boss_reward
    world_boss_rewards: dict[int, WorldBossReward] = {}
    with TaskSessionLocal() as db, db.no_autoflush:  # type: ignore
        transactions = db.query(Transaction).filter_by(signer=signer.address)
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
            db.add(world_boss_reward_amount)
        db.commit()


@celery.task()
def upload_prepare_reward_assets(channel_id: str, raid_id: int):
    with TaskSessionLocal() as db:
        assets = get_assets(raid_id, db)
        result = signer.prepare_reward_assets(MINER_URLS[NetworkType.MAIN], assets)
        decoded = bencodex.loads(bytes.fromhex(result))
        client.chat_postMessage(
            channel=channel_id,
            text=f"world boss season {raid_id} prepareRewardAssets\n```plain_value:{decoded}\n\n{result}```",
        )


@celery.task()
def stage_transaction(headless_url: str, nonce: int) -> str:
    with TaskSessionLocal() as db:
        tx = (
            db.query(Transaction)
            .filter(Transaction.signer == signer.address, Transaction.nonce == nonce)
            .one()
        )
        tx_id = signer.stage_transaction(headless_url, tx)
        return tx_id


@celery.task()
def send_slack_message(channel_id: str, msg: str):
    client.chat_postMessage(channel=channel_id, text=msg)


@celery.task()
def query_tx_result(headless_url: str, tx_id: str):
    with TaskSessionLocal() as db:
        tx_result = signer.query_transaction_result(headless_url, tx_id, db)
        return tx_id, tx_result


@celery.task()
def upload_tx_result(tx_results: List[Tuple[str, str]], channel_id: str):
    with NamedTemporaryFile(suffix=".csv") as temp_file:
        file_name = temp_file.name
        write_tx_result_csv(file_name, tx_results)
        client.files_upload_v2(
            channels=channel_id,
            title="world_boss_tx_result",
            filename=f"world_boss_tx_result_{datetime.utcnow()}.csv",
            file=file_name,
        )


@celery.task()
def check_signer_balance(headless_url: str, currency: CurrencyDictionary) -> str:
    return signer.query_balance(headless_url, currency)


@celery.task()
def upload_balance_result(balance: List[str], channel_id: str):
    balance_str = "\n".join(balance)
    msg = f"world boss pool balance.\naddress:{signer.address}\n\n{balance_str}"
    send_slack_message(channel_id, msg)
