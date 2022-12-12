import json
import time
import unittest
from datetime import timedelta

import pytest

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
