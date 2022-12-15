import datetime
import json
import time
import unittest
from datetime import timedelta
from unittest.mock import MagicMock

import pytest
from pytest_httpx import HTTPXMock
from slack_sdk.web import SlackResponse

from world_boss.app.cache import set_to_cache, cache_exists


def test_raid_rewards_404(fx_test_client, fx_world_boss_reward_amounts, redisdb):
    req = fx_test_client.get("/raid/1/test/rewards")
    assert req.status_code == 404


@pytest.mark.parametrize(
    "caching",
    [
        True,
        False,
    ],
)
def test_raid_rewards(
    fx_test_client, fx_world_boss_reward_amounts, redisdb, caching: bool
):
    reward = fx_world_boss_reward_amounts[0].reward
    raid_id = reward.raid_id
    avatar_address = fx_world_boss_reward_amounts[0].reward.avatar_address
    if caching:
        cache_key = f"raid_rewards_{avatar_address}_{raid_id}_json"
        set_to_cache(cache_key, json.dumps(reward.as_dict()), timedelta(seconds=1))
    req = fx_test_client.get(f"/raid/{raid_id}/{avatar_address}/rewards")
    assert req.status_code == 200
    assert req.json == reward.as_dict()
    if caching:
        time.sleep(2)
        assert not cache_exists(cache_key)


def test_count_total_users(fx_test_client):
    with unittest.mock.patch("world_boss.app.api.count_users.delay") as m:
        req = fx_test_client.post(
            f"/raid/list/count", data={"text": 1, "channel_id": "channel_id"}
        )
        assert req.status_code == 200
        assert req.json == 200
        m.assert_called_once_with("channel_id", 1)


def test_generate_ranking_rewards_csv(fx_test_client):
    with unittest.mock.patch("world_boss.app.api.get_ranking_rewards.delay") as m:
        req = fx_test_client.post(
            f"/raid/rewards/list", data={"text": "1 2 3", "channel_id": "channel_id"}
        )
        assert req.status_code == 200
        assert req.json == 200
        m.assert_called_once_with("channel_id", 1, 2, 3)


@pytest.mark.parametrize("has_header", [True, False])
def test_prepare_transfer_assets(fx_test_client, has_header: bool):
    content = (
        "raid_id,ranking,agent_address,avatar_address,amount,ticker,decimal_places,target_nonce\n2,1,"
        "0xC36f031aA721f52532BA665Ba9F020e45437D98D,5Ea5755eD86631a4D086CC4Fae41740C8985F1B4,1000000,CRYSTAL,"
        "18,55\n2,1,0xC36f031aA721f52532BA665Ba9F020e45437D98D,5Ea5755eD86631a4D086CC4Fae41740C8985F1B4,3500,"
        "RUNESTONE_FENRIR1,0,55\n2,1,0xC36f031aA721f52532BA665Ba9F020e45437D98D,"
        "5Ea5755eD86631a4D086CC4Fae41740C8985F1B4,1200,RUNESTONE_FENRIR2,0,55\n2,1,"
        "0xC36f031aA721f52532BA665Ba9F020e45437D98D,5Ea5755eD86631a4D086CC4Fae41740C8985F1B4,300,"
        "RUNESTONE_FENRIR3,0,55 "
    )
    mocked_response = MagicMock()
    mocked_response.data = {
        "content": content,
    }
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
        ]
    }
    with unittest.mock.patch(
        "world_boss.app.api.client.files_info", return_value=mocked_response
    ) as m, unittest.mock.patch("world_boss.app.api.sign_transfer_assets.delay") as m2:
        req = fx_test_client.post(
            f"/raid/prepare",
            data={
                "text": "https://planetariumhq.slack.com/files/1/2/test.csv 2022-12-31",
                "channel_id": "channel_id",
            },
        )
        assert req.status_code == 200
        assert req.json == 200
        m.assert_called_once_with(file="2")
        m2.assert_called_once_with(recipient_map, "2022-12-31")
