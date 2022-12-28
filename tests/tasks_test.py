import json
import unittest.mock
from typing import List

import pytest
from pytest_httpx import HTTPXMock

from world_boss.app.data_provider import DATA_PROVIDER_URLS
from world_boss.app.enums import NetworkType
from world_boss.app.kms import MINER_URLS
from world_boss.app.models import Transaction, WorldBossReward, WorldBossRewardAmount
from world_boss.app.stubs import (
    RankingRewardDictionary,
    RankingRewardWithAgentDictionary,
)
from world_boss.app.tasks import (
    count_users,
    get_ranking_rewards,
    insert_world_boss_rewards,
    query_tx_result,
    send_slack_message,
    sign_transfer_assets,
    stage_transaction,
    upload_tx_result,
)


def test_count_users(celery_session_worker, httpx_mock: HTTPXMock):
    httpx_mock.add_response(
        method="POST",
        url=DATA_PROVIDER_URLS[NetworkType.MAIN],
        json={"data": {"worldBossTotalUsers": 100}},
    )
    with unittest.mock.patch("world_boss.app.tasks.client.chat_postMessage") as m:
        count_users.delay("channel_id", 1).get(timeout=5)
        req = httpx_mock.get_request()
        assert req is not None
        m.assert_called_once_with(
            channel="channel_id", text="world boss season 1 total users: 100"
        )


def test_get_ranking_rewards(
    redisdb,
    celery_session_worker,
    httpx_mock: HTTPXMock,
    fx_ranking_rewards,
):
    raid_id = 1
    network_type = NetworkType.MAIN
    offset = 0
    rewards_cache_key = f"world_boss_{raid_id}_{network_type}_{offset}_100"
    addresses_cache_key = f"world_boss_agents_{raid_id}_{network_type}_{offset}_100"

    # get from cache key
    cached_rewards: List[RankingRewardDictionary] = [
        {
            "raider": {
                "address": "5Ea5755eD86631a4D086CC4Fae41740C8985F1B4",
                "ranking": i + 1,
            },
            "rewards": fx_ranking_rewards,
        }
        for i in range(0, 100)
    ]

    # get from service query
    requested_rewards: List[RankingRewardDictionary] = [
        {
            "raider": {
                "address": "01A0b412721b00bFb5D619378F8ab4E4a97646Ca",
                "ranking": 101,
            },
            "rewards": fx_ranking_rewards,
        },
    ]

    cached_addresses: List[RankingRewardWithAgentDictionary] = [
        {
            "raider": {
                "address": "5Ea5755eD86631a4D086CC4Fae41740C8985F1B4",
                "ranking": i + 1,
                "agent_address": "0xC36f031aA721f52532BA665Ba9F020e45437D98D",
            },
            "rewards": fx_ranking_rewards,
        }
        for i in range(0, 100)
    ]
    redisdb.set(rewards_cache_key, json.dumps(cached_rewards))
    httpx_mock.add_response(
        method="POST",
        url=DATA_PROVIDER_URLS[NetworkType.MAIN],
        json={"data": {"worldBossRankingRewards": requested_rewards}},
    )
    redisdb.set(addresses_cache_key, json.dumps(cached_addresses))
    httpx_mock.add_response(
        method="POST",
        url=MINER_URLS[NetworkType.MAIN],
        json={
            "data": {
                "stateQuery": {
                    "arg01A0b412721b00bFb5D619378F8ab4E4a97646Ca": {
                        "agentAddress": "0x9EBD1b4F9DbB851BccEa0CFF32926d81eDf6De52",
                    },
                }
            }
        },
    )

    with unittest.mock.patch("world_boss.app.tasks.client.files_upload_v2") as m:
        get_ranking_rewards.delay("channel_id", raid_id, 101, 1).get(timeout=10)
        m.assert_called_once()
        # skip check file. because file is temp file.
        kwargs = m.call_args.kwargs
        assert kwargs["file"]
        assert kwargs["channels"] == "channel_id"
        assert kwargs["title"] == f"world_boss_{raid_id}_101_1_result"
        assert kwargs["filename"] == f"world_boss_{raid_id}_101_1_result.csv"
    assert redisdb.exists(rewards_cache_key)
    assert redisdb.exists(addresses_cache_key)
    assert redisdb.exists(f"world_boss_{raid_id}_{network_type}_100_1")
    assert redisdb.exists(f"world_boss_agents_{raid_id}_{network_type}_100_1")


@pytest.mark.parametrize(
    "nonce, max_nonce, expected_count", [(1, 1, 0), (1, 2, 0), (2, 1, 1)]
)
def test_sign_transfer_assets(
    celery_session_worker,
    fx_session,
    nonce: int,
    max_nonce: int,
    expected_count: int,
):
    assert fx_session.query(Transaction).count() == 0
    sign_transfer_assets.delay(
        "2022-12-31", nonce, [], "memo", MINER_URLS[NetworkType.INTERNAL], max_nonce
    ).get(timeout=10)
    assert fx_session.query(Transaction).count() == expected_count


def test_insert_world_boss_rewards(celery_session_worker, fx_session):
    content = """3,25,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,150000,CRYSTAL,18,175
    3,25,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,560,RUNESTONE_FENRIR1,0,175
    3,25,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,150,RUNESTONE_FENRIR2,0,175
    3,25,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,40,RUNESTONE_FENRIR3,0,175"""
    tx = Transaction()
    tx.nonce = 175
    tx.signer = "0x2531e5e06cBD11aF54f98D39578990716fFC7dBa"
    tx.tx_id = "tx_id"
    tx.payload = "payload"
    fx_session.add(tx)
    fx_session.commit()
    insert_world_boss_rewards.delay([r.split(",") for r in content.split("\n")]).get(
        timeout=10
    )

    assert len(fx_session.query(Transaction).first().amounts) == 4

    world_boss_reward = fx_session.query(WorldBossReward).first()
    assert world_boss_reward.raid_id == 3
    assert world_boss_reward.ranking == 25
    assert (
        world_boss_reward.agent_address == "0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4"
    )
    assert (
        world_boss_reward.avatar_address == "5b65f5D0e23383FA18d74A62FbEa383c7D11F29d"
    )

    assert len(world_boss_reward.amounts) == 4

    for ticker, amount, decimal_places in [
        ("CRYSTAL", 150000, 18),
        ("RUNESTONE_FENRIR1", 560, 0),
        ("RUNESTONE_FENRIR2", 150, 0),
        ("RUNESTONE_FENRIR3", 40, 0),
    ]:
        world_boss_reward_amount = (
            fx_session.query(WorldBossRewardAmount).filter_by(ticker=ticker).one()
        )
        assert world_boss_reward_amount.decimal_places == decimal_places
        assert world_boss_reward_amount.amount == amount


def test_send_slack_message(
    celery_session_worker,
):
    with unittest.mock.patch("world_boss.app.tasks.client.chat_postMessage") as m:
        send_slack_message.delay("channel_id", "test message").get()
        m.assert_called_once_with(channel="channel_id", text="test message")


@pytest.mark.parametrize("network_type", [NetworkType.MAIN, NetworkType.INTERNAL])
def test_stage_transaction(
    celery_session_worker,
    fx_session,
    httpx_mock: HTTPXMock,
    network_type: NetworkType,
):
    transaction = Transaction()
    transaction.tx_id = "tx_id"
    transaction.signer = "0x2531e5e06cBD11aF54f98D39578990716fFC7dBa"
    transaction.payload = "payload"
    transaction.nonce = 1
    fx_session.add(transaction)
    fx_session.commit()
    url = MINER_URLS[network_type]
    httpx_mock.add_response(
        url=url,
        method="POST",
        json={
            "data": {
                "stageTransaction": "tx_id",
            }
        },
    )
    result = stage_transaction.delay(url, 1).get(timeout=3)
    req = httpx_mock.get_request()
    assert req is not None
    assert result == "tx_id"


def test_query_tx_result(celery_session_worker, fx_session, fx_transactions):
    assert fx_session.query(Transaction).count() == 0
    tx_ids = []
    for transaction in fx_transactions:
        fx_session.add(transaction)
        tx_ids.append(transaction.tx_id)
    fx_session.commit()

    for tx_id in tx_ids:
        _, result = query_tx_result.delay(MINER_URLS[NetworkType.MAIN], tx_id).get(
            timeout=10
        )
        tx = fx_session.query(Transaction).filter_by(tx_id=tx_id).one()
        assert result == "SUCCESS"
        assert tx.tx_result == "SUCCESS"


def test_upload_result(
    celery_session_worker,
):
    with unittest.mock.patch("world_boss.app.tasks.client.files_upload_v2") as m:
        upload_tx_result.delay([("tx_id", "SUCCESS")], "channel_id").get(timeout=30)
        m.assert_called_once()
        # skip check file. because file is temp file.
        kwargs = m.call_args.kwargs
        assert kwargs["file"]
        assert kwargs["channels"] == "channel_id"
        assert kwargs["title"] == "world_boss_tx_result"
        assert "world_boss_tx_result" in kwargs["filename"]
