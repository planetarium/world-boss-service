import datetime
import json
import unittest.mock
from typing import List

from pytest_httpx import HTTPXMock

from world_boss.app.data_provider import DATA_PROVIDER_URLS
from world_boss.app.enums import NetworkType
from world_boss.app.kms import MINER_URLS
from world_boss.app.models import Transaction
from world_boss.app.stubs import (
    RankingRewardDictionary,
    RewardDictionary,
    RankingRewardWithAgentDictionary,
)
from world_boss.app.tasks import count_users, get_ranking_rewards, sign_transfer_assets


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


def test_sign_transfer_assets(redisdb, celery_app, celery_worker, fx_app, fx_session):
    assert not fx_session.query(Transaction).first()
    recipient_map = {
        55: [
            {
                "amount": {
                    "decimalPlaces": 18,
                    "quantity": 1000000,
                    "ticker": "CRYSTAL",
                },
                "recipient": "0xC36f031aA721f52532BA665Ba9F020e45437D98D",
            },
            {
                "amount": {
                    "decimalPlaces": 0,
                    "quantity": 3500,
                    "ticker": "RUNESTONE_FENRIR1",
                },
                "recipient": "5Ea5755eD86631a4D086CC4Fae41740C8985F1B4",
            },
            {
                "amount": {
                    "decimalPlaces": 0,
                    "quantity": 1200,
                    "ticker": "RUNESTONE_FENRIR2",
                },
                "recipient": "5Ea5755eD86631a4D086CC4Fae41740C8985F1B4",
            },
            {
                "amount": {
                    "decimalPlaces": 0,
                    "quantity": 300,
                    "ticker": "RUNESTONE_FENRIR3",
                },
                "recipient": "5Ea5755eD86631a4D086CC4Fae41740C8985F1B4",
            },
        ],
        56: [
            {
                "amount": {
                    "decimalPlaces": 18,
                    "quantity": 1000000,
                    "ticker": "CRYSTAL",
                },
                "recipient": "0x9EBD1b4F9DbB851BccEa0CFF32926d81eDf6De52",
            },
            {
                "amount": {
                    "decimalPlaces": 0,
                    "quantity": 3500,
                    "ticker": "RUNESTONE_FENRIR1",
                },
                "recipient": "01A0b412721b00bFb5D619378F8ab4E4a97646Ca",
            },
            {
                "amount": {
                    "decimalPlaces": 0,
                    "quantity": 1200,
                    "ticker": "RUNESTONE_FENRIR2",
                },
                "recipient": "01A0b412721b00bFb5D619378F8ab4E4a97646Ca",
            },
            {
                "amount": {
                    "decimalPlaces": 0,
                    "quantity": 300,
                    "ticker": "RUNESTONE_FENRIR3",
                },
                "recipient": "01A0b412721b00bFb5D619378F8ab4E4a97646Ca",
            },
        ],
    }
    sign_transfer_assets.delay(
        recipient_map, datetime.datetime(2023, 1, 31).isoformat()
    ).get(timeout=30)
    # sign_transfer_assets(recipient_map, datetime.datetime(2023, 1, 31))
    assert fx_session.query(Transaction).count() == 2
