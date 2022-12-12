import json
from typing import cast, List

import httpx
from flask import jsonify

from world_boss.app.cache import set_to_cache, cache_exists, get_from_cache
from world_boss.app.enums import NetworkType
from world_boss.app.kms import MINER_URLS
from world_boss.app.models import WorldBossReward
from world_boss.app.stubs import (
    RankingRewardDictionary,
    RankingRewardWithAgentDictionary,
)


def get_raid_rewards(raid_id: int, avatar_address: str):
    avatar_address = avatar_address.replace("0x", "")

    cache_key = f"raid_rewards_{avatar_address}_{raid_id}_json"
    if cache_exists(cache_key):
        cached_value = get_from_cache(cache_key)
        cached_result = json.loads(cached_value)
        resp = jsonify(cached_result)
        resp.headers["X-world-boss-service-response-cached"] = cache_key
        return resp

    reward = WorldBossReward.query.filter_by(
        raid_id=raid_id,
        avatar_address=avatar_address,
    ).first_or_404()
    result = reward.as_dict()
    set_to_cache(cache_key, json.dumps(result))
    return jsonify(result)


def update_agent_address(
    results: List[RankingRewardDictionary],
    raid_id: int,
    network_type: NetworkType,
    offset: int,
    limit: int,
) -> List[RankingRewardWithAgentDictionary]:
    cache_key = f"world_boss_agents_{raid_id}_{network_type}_{offset}_{limit}"
    if cache_exists(cache_key):
        cached_value = get_from_cache(cache_key)
        return json.loads(cached_value)
    else:
        http_client = httpx.Client(timeout=30)
        rewards: List[RankingRewardWithAgentDictionary] = []
        query_keys = [r["raider"]["address"] for r in results]
        alias_key_format = "arg{}"
        variables_format = "$arg{}: Address!"
        query_format = """
        {0}: avatar(avatarAddress: ${0}) {{
              agentAddress
              level
              name
            }}
        """
        base_query_format = """
        query({}) {{
          stateQuery {{
          {}
          }}
        }}
        """
        variables_keys = ", ".join(variables_format.format(a) for a in query_keys)
        avatar_queries = "\n".join(
            query_format.format(alias_key_format.format(i)) for i in query_keys
        )
        query = base_query_format.format(variables_keys, avatar_queries)
        variables = {}
        for avatar_address in query_keys:
            variables[f"arg{avatar_address}"] = avatar_address
        req = http_client.post(
            MINER_URLS[NetworkType.MAIN], json={"query": query, "variables": variables}
        )
        query_result = req.json()
        agents = query_result["data"]["stateQuery"]
        for result in results:
            avatar_address = result["raider"]["address"]
            r: RankingRewardWithAgentDictionary = cast(
                RankingRewardWithAgentDictionary, result
            )
            r["raider"]["agent_address"] = agents[f"arg{avatar_address}"][
                "agentAddress"
            ]
            rewards.append(r)
        set_to_cache(cache_key, json.dumps(rewards))
        return rewards
