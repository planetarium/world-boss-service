import datetime
import json
import unittest.mock
from typing import List

import bencodex
import pytest
from pytest_httpx import HTTPXMock

from world_boss.app.data_provider import DATA_PROVIDER_URLS
from world_boss.app.enums import NetworkType
from world_boss.app.kms import MINER_URLS
from world_boss.app.models import Transaction, WorldBossReward, WorldBossRewardAmount
from world_boss.app.stubs import (
    AmountDictionary,
    RankingRewardDictionary,
    RankingRewardWithAgentDictionary,
)
from world_boss.app.tasks import (
    count_users,
    get_ranking_rewards,
    insert_world_boss_rewards,
    prepare_world_boss_ranking_rewards,
    sign_transfer_assets,
    upload_prepare_reward_assets,
)


def test_count_users(fx_app, redisdb, celery_app, celery_worker, httpx_mock: HTTPXMock):
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
    fx_app,
    redisdb,
    celery_app,
    celery_worker,
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
    redisdb,
    celery_app,
    celery_worker,
    fx_app,
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


def test_insert_world_boss_rewards(
    redisdb, celery_app, celery_worker, fx_app, fx_session
):
    content = """3,25,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,150000,CRYSTAL,18,175
    3,25,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,560,RUNESTONE_FENRIR1,0,175
    3,25,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,150,RUNESTONE_FENRIR2,0,175
    3,25,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,40,RUNESTONE_FENRIR3,0,175"""
    tx = Transaction()
    tx.nonce = 175
    tx.signer = "0xCFCd6565287314FF70e4C4CF309dB701C43eA5bD"
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


def test_prepare_world_boss_ranking_rewards(
    redisdb, celery_app, celery_worker, fx_app, fx_session
):
    assert not fx_session.query(Transaction).first()
    assert not fx_session.query(WorldBossReward).first()
    assert not fx_session.query(WorldBossRewardAmount).first()
    content = """3,25,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,150000,CRYSTAL,18,175
3,25,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,560,RUNESTONE_FENRIR1,0,175
3,25,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,150,RUNESTONE_FENRIR2,0,175
3,25,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,40,RUNESTONE_FENRIR3,0,175
3,26,0x1774cd5d2C1C0f72AA75E9381889a1a554797a4c,1F8d5e0D201B7232cE3BC8d630d09E3F9107CceE,150000,CRYSTAL,18,176
3,26,0x1774cd5d2C1C0f72AA75E9381889a1a554797a4c,1F8d5e0D201B7232cE3BC8d630d09E3F9107CceE,560,RUNESTONE_FENRIR1,0,176
3,26,0x1774cd5d2C1C0f72AA75E9381889a1a554797a4c,1F8d5e0D201B7232cE3BC8d630d09E3F9107CceE,150,RUNESTONE_FENRIR2,0,176
3,26,0x1774cd5d2C1C0f72AA75E9381889a1a554797a4c,1F8d5e0D201B7232cE3BC8d630d09E3F9107CceE,40,RUNESTONE_FENRIR3,0,176"""
    prepare_world_boss_ranking_rewards.delay(
        [r.split(",") for r in content.split("\n")],
        datetime.datetime(2023, 1, 31).isoformat(),
    ).get(timeout=30)
    expected = [
        {
            "nonce": 175,
            "ranking": 25,
            "agent_address": "0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4",
            "avatar_address": "5b65f5D0e23383FA18d74A62FbEa383c7D11F29d",
        },
        {
            "nonce": 176,
            "ranking": 26,
            "agent_address": "0x1774cd5d2C1C0f72AA75E9381889a1a554797a4c",
            "avatar_address": "1F8d5e0D201B7232cE3BC8d630d09E3F9107CceE",
        },
    ]
    reward_amounts = [
        {
            "ticker": "CRYSTAL",
            "decimal_places": 18,
            "amount": 150000,
        },
        {
            "ticker": "RUNESTONE_FENRIR1",
            "decimal_places": 0,
            "amount": 560,
        },
        {
            "ticker": "RUNESTONE_FENRIR2",
            "decimal_places": 0,
            "amount": 150,
        },
        {
            "ticker": "RUNESTONE_FENRIR3",
            "decimal_places": 0,
            "amount": 40,
        },
    ]
    for i, tx in enumerate(fx_session.query(Transaction).order_by(Transaction.nonce)):
        assert tx.nonce == expected[i]["nonce"]
        assert tx.tx_result is None
        assert len(tx.amounts) == 4

        world_boss_reward = tx.amounts[0].reward
        assert world_boss_reward.raid_id == 3
        assert world_boss_reward.ranking == expected[i]["ranking"]
        assert world_boss_reward.agent_address == expected[i]["agent_address"]
        assert world_boss_reward.avatar_address == expected[i]["avatar_address"]

        for v, world_boss_reward_amount in enumerate(tx.amounts):
            assert world_boss_reward_amount.ticker == reward_amounts[v]["ticker"]
            assert (
                world_boss_reward_amount.decimal_places
                == reward_amounts[v]["decimal_places"]
            )
            assert world_boss_reward_amount.amount == reward_amounts[v]["amount"]


@pytest.mark.parametrize("raid_id", [1, 2])
def test_upload_prepare_reward_assets(
    redisdb,
    celery_app,
    celery_worker,
    fx_app,
    fx_session,
    httpx_mock: HTTPXMock,
    raid_id: int,
):
    result = []
    assets: List[AmountDictionary] = [
        {"decimalPlaces": 18, "ticker": "CRYSTAL", "quantity": 109380000},
        {"decimalPlaces": 0, "ticker": "RUNESTONE_FENRIR1", "quantity": 406545},
        {"decimalPlaces": 0, "ticker": "RUNESTONE_FENRIR2", "quantity": 111715},
        {"decimalPlaces": 0, "ticker": "RUNESTONE_FENRIR3", "quantity": 23890},
    ]
    reward = WorldBossReward()
    reward.avatar_address = "avatar_address"
    reward.agent_address = "agent_address"
    reward.raid_id = raid_id
    reward.ranking = 1

    for i, asset in enumerate(assets):
        reward_amount = WorldBossRewardAmount()
        reward_amount.amount = asset["quantity"]
        reward_amount.ticker = asset["ticker"]
        reward_amount.decimal_places = asset["decimalPlaces"]
        reward_amount.reward = reward
        tx_id = i
        reward_amount.tx_id = tx_id
        transaction = Transaction()
        transaction.tx_id = tx_id
        transaction.signer = "signer"
        transaction.payload = "payload"
        transaction.nonce = i
        fx_session.add(transaction)
        result.append(reward_amount)
    fx_session.commit()
    raw = "6475373a747970655f69647532313a707265706172655f7265776172645f61737365747375363a76616c7565736475313a616c6c647531333a646563696d616c506c61636573313a1275373a6d696e746572736e75363a7469636b657275373a4352595354414c656931303933383030303030303030303030303030303030303030303065656c647531333a646563696d616c506c61636573313a0075373a6d696e746572736e75363a7469636b65727531373a52554e4553544f4e455f46454e52495231656934303635343565656c647531333a646563696d616c506c61636573313a0075373a6d696e746572736e75363a7469636b65727531373a52554e4553544f4e455f46454e52495232656931313137313565656c647531333a646563696d616c506c61636573313a0075373a6d696e746572736e75363a7469636b65727531373a52554e4553544f4e455f46454e524952336569323338393065656575313a7232303a2531e5e06cbd11af54f98d39578990716ffc7dba6565"
    httpx_mock.add_response(
        method="POST",
        url=MINER_URLS[NetworkType.MAIN],
        json={"data": {"actionQuery": {"prepareRewardAssets": raw}}},
    )
    with unittest.mock.patch("world_boss.app.tasks.client.chat_postMessage") as m:
        upload_prepare_reward_assets.delay("channel_id", raid_id).get()
        req = httpx_mock.get_request()
        assert req is not None
        m.assert_called_once_with(
            channel="channel_id",
            text=f"world boss season {raid_id} prepareRewardAssets\n```plain_value:{bencodex.loads(bytes.fromhex(raw))}\n\n{raw}```",
        )
