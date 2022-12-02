import pickle

from flask import jsonify

from world_boss.app.cache import rd, set_to_cache, cache_exists
from world_boss.app.models import WorldBossReward


def get_raid_rewards(raid_id: int, avatar_address: str):
    avatar_address = avatar_address.replace('0x', '')

    cache_key = f'raid_rewards_{avatar_address}_{raid_id}'
    if cache_exists(cache_key):
        cached_result = pickle.loads(rd.get(cache_key))
        resp = jsonify(cached_result)
        resp.headers['X-9c-onboarding-portal-response-cached'] = cache_key
        return resp

    reward = WorldBossReward.query.filter_by(
        raid_id=raid_id,
        avatar_address=avatar_address,
    ).first_or_404()
    result = reward.as_dict()
    set_to_cache(cache_key, pickle.dumps(result))
    return jsonify(result)
