import csv
import json
from typing import Union, cast, List

from flask import jsonify

from world_boss.app.cache import rd, set_to_cache, cache_exists
from world_boss.app.data_provider import data_provider_client
from world_boss.app.enums import NetworkType
from world_boss.app.models import WorldBossReward
from world_boss.app.stubs import (
    RaiderDictionary,
    RewardDictionary,
    CurrencyDictionary,
    RankingRewardDictionary,
)


def get_raid_rewards(raid_id: int, avatar_address: str):
    avatar_address = avatar_address.replace("0x", "")

    cache_key = f"raid_rewards_{avatar_address}_{raid_id}_json"
    if cache_exists(cache_key):
        cached_value = cast(Union[str, bytes], rd.get(cache_key))
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


async def to_reward_file(raid_id: int, file_path: str):
    result: List[
        RankingRewardDictionary
    ] = await data_provider_client.get_ranking_rewards(raid_id, NetworkType.MAIN)
    with open(file_path, "w") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["ranking", "avatar_address", "amount", "ticker", "decimal_places"]
        )
        for r in result:
            raider: RaiderDictionary = r["raider"]
            ranking = raider["ranking"]
            avatar_address = raider["address"]
            rewards: List[RewardDictionary] = r["rewards"]
            for reward in rewards:
                currency: CurrencyDictionary = reward["currency"]
                amount = reward["quantity"]
                writer.writerow(
                    [
                        ranking,
                        avatar_address,
                        amount,
                        currency["ticker"],
                        currency["decimalPlaces"],
                    ]
                )
