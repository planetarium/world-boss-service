import json
import unittest
from typing import List

import pytest
from pytest_httpx import HTTPXMock

from world_boss.app.cache import cache_exists, set_to_cache
from world_boss.app.data_provider import DATA_PROVIDER_URLS, data_provider_client
from world_boss.app.enums import NetworkType
from world_boss.app.stubs import RankingRewardDictionary


@pytest.mark.parametrize("network_type", [NetworkType.MAIN, NetworkType.INTERNAL])
@pytest.mark.parametrize("raid_id, expected_count", [(1, 1958), (2, 1841)])
def test_get_total_users_count(
    network_type: NetworkType, raid_id: int, expected_count: int
):
    result = data_provider_client.get_total_users_count(raid_id, network_type)
    assert result == expected_count


@pytest.mark.parametrize("network_type", [NetworkType.MAIN, NetworkType.INTERNAL])
@pytest.mark.parametrize("raid_id", [1, 2])
@pytest.mark.parametrize("caching", [True, False])
def test_get_ranking_rewards(
    redisdb, network_type: NetworkType, raid_id: int, caching: bool, fx_ranking_rewards
):
    offset = 0
    limit = 1
    cache_key = f"world_boss_{raid_id}_{network_type}_{offset}_{limit}"
    expected_result: List[RankingRewardDictionary] = [
        {
            "raider": {
                "address": "5Ea5755eD86631a4D086CC4Fae41740C8985F1B4",
                "ranking": 1,
            },
            "rewards": fx_ranking_rewards,
        }
    ]
    if caching:
        set_to_cache(cache_key, json.dumps(expected_result))
    result = data_provider_client.get_ranking_rewards(raid_id, network_type, 0, 1)
    assert result == expected_result
    assert cache_exists(cache_key)
