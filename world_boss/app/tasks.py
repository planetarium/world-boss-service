import json
from datetime import datetime
from tempfile import NamedTemporaryFile
from typing import List, Tuple

import bencodex
from celery import Celery, chord
from sqlalchemy import create_engine, insert
from sqlalchemy.orm import sessionmaker

from world_boss.app.cache import cache_exists, set_to_cache
from world_boss.app.config import config
from world_boss.app.data_provider import data_provider_client
from world_boss.app.enums import NetworkType
from world_boss.app.kms import signer
from world_boss.app.models import Transaction, WorldBossReward, WorldBossRewardAmount
from world_boss.app.raid import (
    bulk_insert_transactions,
    get_assets,
    get_latest_raid_id,
    get_next_month_last_day,
    get_next_tx_nonce,
    get_prepare_reward_assets_plain_value,
    get_reward_count,
    get_tx_delay_factor,
    update_agent_address,
    write_ranking_rewards_csv,
    write_tx_result_csv,
)
from world_boss.app.slack import client
from world_boss.app.stubs import (
    CurrencyDictionary,
    RaiderWithAgentDictionary,
    RankingRewardWithAgentDictionary,
    Recipient,
    RecipientRow,
    RewardDictionary,
)

celery = Celery()
celery.conf.broker_url = config.celery_broker_url
celery.conf.result_backend = config.celery_result_backend
celery.conf.timezone = "UTC"

task_engine = create_engine(str(config.database_url))
TaskSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=task_engine)


@celery.task()
def count_users(channel_id: str, raid_id: int):
    total_count = data_provider_client.get_total_users_count(raid_id)
    client.chat_postMessage(
        channel=channel_id,
        text=f"world boss season {raid_id} total users: {total_count}",
    )


@celery.task()
def get_ranking_rewards(
    channel_id: str, raid_id: int, total_count: int, start_nonce: int, size: int
):
    offset = 0
    results: List[RankingRewardWithAgentDictionary] = []
    payload_size = size
    while len(results) < total_count:
        try:
            result = data_provider_client.get_ranking_rewards(
                raid_id, NetworkType.MAIN, offset, payload_size
            )
        except Exception as e:
            client.chat_postMessage(
                channel=channel_id,
                text=f"failed to get rewards from {config.data_provider_url} exc: {e}",
            )
            raise e
        rewards = update_agent_address(
            result, raid_id, NetworkType.MAIN, offset, payload_size
        )
        results.extend(reward for reward in rewards if reward not in results)
        offset = len(results)
        payload_size = min(payload_size, total_count - offset)
    with NamedTemporaryFile(suffix=".csv") as temp_file:
        file_name = temp_file.name
        write_ranking_rewards_csv(file_name, results, raid_id, start_nonce, size)
        result_format = (
            f"world_boss_{raid_id}_{total_count}_{start_nonce}_{size}_result"
        )
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
            signer.transfer_assets(time_stamp, nonce, recipients, memo, db)


@celery.task()
def insert_world_boss_rewards(rows: List[RecipientRow], signer_address: str):
    # ranking : world_boss_reward
    world_boss_rewards: dict[int, dict] = {}
    with TaskSessionLocal() as db, db.no_autoflush:  # type: ignore
        raid_id = int(rows[0][0])
        exist_rankings = [
            r for r, in db.query(WorldBossReward.ranking).filter_by(raid_id=raid_id)
        ]
        transactions = db.query(Transaction).filter_by(signer=signer_address)
        world_boss_reward_amounts: dict[int, list[dict]] = {}
        # raid_id,ranking,agent_address,avatar_address,amount,ticker,decimal_places,target_nonce
        for row in rows:
            # parse row
            ranking = int(row[1])
            agent_address = row[2]
            avatar_address = row[3]
            amount = int(row[4])
            ticker = row[5]
            decimal_places = int(row[6])
            nonce = int(row[7])

            # get or create world_boss_reward
            if ranking not in exist_rankings and not world_boss_rewards.get(ranking):
                world_boss_reward = {
                    "raid_id": raid_id,
                    "ranking": ranking,
                    "agent_address": agent_address,
                    "avatar_address": avatar_address,
                }
                world_boss_rewards[ranking] = world_boss_reward

            # create world_boss_reward_amount
            world_boss_reward_amount = {
                "amount": amount,
                "decimal_places": decimal_places,
                "ticker": ticker,
                "tx_id": transactions.filter_by(nonce=nonce).one().tx_id,
            }
            if not world_boss_reward_amounts.get(ranking):
                world_boss_reward_amounts[ranking] = []
            world_boss_reward_amounts[ranking].append(world_boss_reward_amount)
        if world_boss_rewards:
            db.execute(insert(WorldBossReward), world_boss_rewards.values())
        result = db.query(WorldBossReward).filter_by(raid_id=raid_id)
        values = []
        for reward in result:
            exist_tickers = [i.ticker for i in reward.amounts]
            if world_boss_reward_amounts.get(reward.ranking):
                for amounts in world_boss_reward_amounts[reward.ranking]:
                    if amounts["ticker"] not in exist_tickers:
                        amounts["reward_id"] = reward.id
                        values.append(amounts)
        if values:
            db.execute(insert(WorldBossRewardAmount), values)
            db.commit()


@celery.task()
def upload_prepare_reward_assets(channel_id: str, raid_id: int):
    with TaskSessionLocal() as db:
        assets = get_assets(raid_id, db)
        result = get_prepare_reward_assets_plain_value(signer.address, assets)
        serialized = bencodex.dumps(result).hex()
        client.chat_postMessage(
            channel=channel_id,
            text=f"world boss season {raid_id} prepareRewardAssets\n```plain_value:{result}\n\n{serialized}```",
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


@celery.task()
def stage_transactions_with_countdown(headless_url: str, nonce_list: List[int]):
    chord(
        stage_transaction.signature(
            (headless_url, nonce), countdown=get_tx_delay_factor(i)
        )
        for i, nonce in enumerate(nonce_list)
    )(
        send_slack_message.si(
            config.slack_channel_id, f"stage {len(nonce_list)} transactions"
        )
    )


@celery.task()
def check_season():
    with TaskSessionLocal() as db:
        raid_id = get_latest_raid_id(db)
        total_count = data_provider_client.get_total_users_count(raid_id)
        sync_count = offset = get_reward_count(db, raid_id)
        # 최신 시즌 동기화 처리
        if sync_count == total_count:
            upload_tx_list(raid_id)
            raid_id += 1
            offset = 0
        save_ranking_rewards(
            raid_id=raid_id, payload_size=500, recipients_size=50, offset=offset
        )


@celery.task()
def save_ranking_rewards(
    raid_id: int, payload_size: int, recipients_size: int, offset: int
):
    """

    :param raid_id: target season id
    :param payload_size: request payload size to data provider
    :param recipients_size: transfer_assets recipients size each tx
    :param offset: query offset to data provider
    """
    results: List[RankingRewardWithAgentDictionary] = []
    time_stamp = get_next_month_last_day()
    memo = "world boss ranking rewards by world boss signer"
    with TaskSessionLocal() as db:
        start_nonce = get_next_tx_nonce(db)
        result = data_provider_client.get_ranking_rewards(
            raid_id, NetworkType.MAIN, offset, payload_size
        )
        rewards = update_agent_address(
            result, raid_id, NetworkType.MAIN, offset, payload_size
        )
        results.extend(reward for reward in rewards if reward not in results)
        nonce_rows_map: dict[int, List[RecipientRow]] = {}
        rows: List[RecipientRow] = []
        i = 0
        for r in results:
            raider: RaiderWithAgentDictionary = r["raider"]
            ranking = raider["ranking"]
            avatar_address = raider["address"]
            reward_dict_list: List[RewardDictionary] = r["rewards"]
            for reward_dict in reward_dict_list:
                nonce = start_nonce + int(i / recipients_size)
                if not nonce_rows_map.get(nonce):
                    nonce_rows_map[nonce] = []
                currency: CurrencyDictionary = reward_dict["currency"]
                amount = reward_dict["quantity"]
                row: RecipientRow = [
                    str(raid_id),
                    str(ranking),
                    raider["agent_address"],
                    avatar_address,
                    amount,
                    currency["ticker"],
                    str(currency["decimalPlaces"]),
                    str(nonce),
                ]
                rows.append(row)
                nonce_rows_map[nonce].append(row)
                i += 1
        bulk_insert_transactions(rows, nonce_rows_map, time_stamp, db, signer, memo)


@celery.task()
def upload_tx_list(raid_id: int):
    """
    upload signed tx csv data on Slack channel.
    :param raid_id: target world boss season
    """
    cache_key = f"{raid_id}_uploaded"
    # 중복 업로드 방지
    if cache_exists(cache_key):
        return
    with TaskSessionLocal() as db:
        results: dict[str, RankingRewardWithAgentDictionary] = {}
        query = (
            db.query(
                WorldBossReward.avatar_address,
                WorldBossReward.ranking,
                WorldBossReward.agent_address,
                WorldBossRewardAmount.decimal_places,
                WorldBossRewardAmount.ticker,
                WorldBossRewardAmount.amount,
                Transaction.nonce,
            )
            .join(
                WorldBossRewardAmount,
                WorldBossReward.id == WorldBossRewardAmount.reward_id,
            )
            .join(Transaction, Transaction.tx_id == WorldBossRewardAmount.tx_id)
            .filter(WorldBossReward.raid_id == raid_id)
            .order_by(Transaction.nonce, WorldBossReward.ranking)
        )
        total_count = (
            db.query(WorldBossReward.avatar_address).filter_by(raid_id=raid_id).count()
        )
        size = 50
        start_nonce = query.first().nonce
        channel_id = config.slack_channel_id
        for (
            avatar_address,
            ranking,
            agent_address,
            decimal_places,
            ticker,
            amount,
            _,
        ) in query:
            r: RankingRewardWithAgentDictionary
            reward_dictionary: RewardDictionary = {
                "currency": {
                    "decimalPlaces": decimal_places,
                    "minters": None,
                    "ticker": ticker,
                },
                "quantity": str(amount),
            }
            if results.get(avatar_address):
                r = results[avatar_address]
            else:
                r = {
                    "raider": {
                        "address": avatar_address,
                        "ranking": ranking,
                        "agent_address": agent_address,
                    },
                    "rewards": [],
                }
            r["rewards"].append(reward_dictionary)
            results[avatar_address] = r
        with NamedTemporaryFile(suffix=".csv") as temp_file:
            file_name = temp_file.name
            values = list(results.values())
            write_ranking_rewards_csv(file_name, values, raid_id, start_nonce, size)
            result_format = (
                f"world_boss_{raid_id}_{total_count}_{start_nonce}_{size}_result"
            )
            client.files_upload_v2(
                channels=channel_id,
                title=result_format,
                filename=f"{result_format}.csv",
                file=file_name,
                initial_comment="test",
            )
            set_to_cache(cache_key, json.dumps(values), None)
