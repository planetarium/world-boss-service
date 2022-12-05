import json
import time
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
