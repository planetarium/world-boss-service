from typing import List

import pytest

from world_boss.app.cache import cache_exists
from world_boss.app.enums import NetworkType
from world_boss.app.raid import update_agent_address
from world_boss.app.stubs import (
    RankingRewardDictionary,
    RankingRewardWithAgentDictionary,
)


@pytest.mark.parametrize("network_type", [NetworkType.MAIN, NetworkType.INTERNAL])
@pytest.mark.parametrize("caching", [True, False])
@pytest.mark.parametrize("offset", [0, 1])
@pytest.mark.parametrize("limit", [1, 2])
def test_update_agent_address(
    redisdb, network_type: NetworkType, caching: bool, offset: int, limit: int
):
    cache_key = f"world_boss_agents_1_{network_type}_{offset}_{limit}"
    rewards: List[RankingRewardDictionary] = [
        {
            "raider": {
                "address": "5Ea5755eD86631a4D086CC4Fae41740C8985F1B4",
                "ranking": 1,
            },
            "rewards": [],
        },
        {
            "raider": {
                "address": "01A0b412721b00bFb5D619378F8ab4E4a97646Ca",
                "ranking": 2,
            },
            "rewards": [],
        },
    ]
    expected_result: List[RankingRewardWithAgentDictionary] = [
        {
            "raider": {
                "address": "5Ea5755eD86631a4D086CC4Fae41740C8985F1B4",
                "ranking": 1,
                "agent_address": "0xC36f031aA721f52532BA665Ba9F020e45437D98D",
            },
            "rewards": [],
        },
        {
            "raider": {
                "address": "01A0b412721b00bFb5D619378F8ab4E4a97646Ca",
                "ranking": 2,
                "agent_address": "0x9EBD1b4F9DbB851BccEa0CFF32926d81eDf6De52",
            },
            "rewards": [],
        },
    ]
    result: List[RankingRewardWithAgentDictionary] = update_agent_address(
        rewards, 1, network_type, offset, limit
    )
    assert result == expected_result
    assert cache_exists(cache_key)
