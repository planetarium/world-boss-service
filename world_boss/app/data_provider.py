import asyncio
import itertools
from typing import List

import httpx

from world_boss.app.enums import NetworkType

__all__ = ["DataProviderClient", "data_provider_client"]

from world_boss.app.stubs import RankingRewardDictionary

TOTAL_USER_QUERY = "query($raidId: Int!) { worldBossTotalUsers(raidId: $raidId) }"
DATA_PROVIDER_URLS: dict[NetworkType, str] = {
    NetworkType.MAIN: "https://api.9c.gg/graphql",
    NetworkType.INTERNAL: "https://api.9c.gg/graphql",
}
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
        self.client = httpx.AsyncClient()

    async def _query(self, network_type: NetworkType, query: str, variables: dict):
        result = await self.client.post(
            DATA_PROVIDER_URLS[network_type],
            json={"query": query, "variables": variables},
        )
        return result.json()

    async def get_total_users_count(
        self, raid_id: int, network_type: NetworkType
    ) -> int:
        variables = {"raidId": raid_id}
        result = await self._query(network_type, TOTAL_USER_QUERY, variables)
        return result["data"]["worldBossTotalUsers"]

    async def get_ranking_rewards(
        self, raid_id: int, network_type: NetworkType
    ) -> List[RankingRewardDictionary]:
        total_count = await self.get_total_users_count(raid_id, network_type)
        offset = 0
        limit = 100
        values = []
        while offset < total_count:
            values.append(offset)
            if offset + limit < total_count:
                offset += limit
            else:
                offset = total_count - 1
                break
        result = await asyncio.gather(
            *[
                self._get_ranking_rewards(raid_id, network_type, offset=o, limit=limit)
                for o in values
            ]
        )
        chained = list(itertools.chain(*result))
        return chained

    async def _get_ranking_rewards(
        self, raid_id: int, network_type: NetworkType, offset: int, limit: int
    ):
        result = await self._query(
            network_type,
            RANKING_REWARDS_QUERY,
            {"raidId": raid_id, "offset": offset, "limit": limit},
        )
        return result["data"]["worldBossRankingRewards"]


data_provider_client = DataProviderClient()
