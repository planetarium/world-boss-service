import json
from typing import List

import httpx

from world_boss.app.config import config
from world_boss.app.enums import NetworkType

__all__ = ["DataProviderClient", "data_provider_client"]

from world_boss.app.cache import cache_exists, get_from_cache, set_to_cache
from world_boss.app.stubs import RankingRewardDictionary

TOTAL_USER_QUERY = "query($raidId: Int!) { worldBossTotalUsers(raidId: $raidId) }"
DATA_PROVIDER_URL: str = config.data_provider_url
RANKING_REWARDS_QUERY = """
    query($raidId: Int!, $limit: Int!, $offset: Int!) {
        worldBossRankingRewards(raidId: $raidId, limit: $limit, offset: $offset) {
            raider {
                address
                ranking
            }
            rewards {
                currency {
                    ticker
                    decimalPlaces
                    minters
                }
                quantity
            }
        }
    }
"""


class DataProviderClient:
    def __init__(self):
        self._client = httpx.Client(timeout=None)

    def _query(self, query: str, variables: dict):
        result = self._client.post(
            DATA_PROVIDER_URL,
            json={"query": query, "variables": variables},
        )
        return result.json()

    def get_total_users_count(self, raid_id: int) -> int:
        variables = {"raidId": raid_id}
        result = self._query(TOTAL_USER_QUERY, variables)
        return result["data"]["worldBossTotalUsers"]

    def get_ranking_rewards(
        self, raid_id: int, network_type: NetworkType, offset: int, limit: int
    ) -> List[RankingRewardDictionary]:
        cache_key = f"world_boss_{raid_id}_{network_type}_{offset}_{limit}"
        if cache_exists(cache_key):
            cached_value = get_from_cache(cache_key)
            rewards = json.loads(cached_value)
        else:
            result = self._query(
                RANKING_REWARDS_QUERY,
                {"raidId": raid_id, "offset": offset, "limit": limit},
            )
            if result.get("errors"):
                raise RankingRewardsException(result["errors"][0]["message"])
            rewards = result["data"]["worldBossRankingRewards"]
            set_to_cache(cache_key, json.dumps(rewards))
        return rewards


class RankingRewardsException(Exception):
    pass


data_provider_client = DataProviderClient()
