from flask import Blueprint

from world_boss.app.raid import get_raid_rewards

api = Blueprint('api', __name__)


@api.route('/ping')
def pong() -> str:
    return 'pong'


@api.route('/raid/<raid_id>/<avatar_address>/rewards', methods=['GET'])
def raid_rewards(raid_id: str, avatar_address: str):
    raid_id = int(raid_id)
    return get_raid_rewards(raid_id, avatar_address)
