from flask import Blueprint, request, jsonify
from world_boss.app.raid import get_raid_rewards
from world_boss.app.tasks import count_users, get_ranking_rewards

api = Blueprint("api", __name__)


@api.route("/ping")
def pong() -> str:
    return "pong"


@api.route("/raid/<raid_id>/<avatar_address>/rewards", methods=["GET"])
def raid_rewards(raid_id: int, avatar_address: str):
    return get_raid_rewards(raid_id, avatar_address)


@api.post("/raid/list/count")
def count_total_users():
    raid_id = request.values.get("text", type=int)
    channel_id = request.values.get("channel_id")
    count_users.delay(channel_id, raid_id)
    return jsonify(200)


@api.post("/raid/rewards/list")
def generate_ranking_rewards_csv():
    values = request.values.get("text").split()
    raid_id, total_users, nonce = [int(v) for v in values]
    channel_id = request.values.get("channel_id")
    get_ranking_rewards.delay(channel_id, raid_id, total_users, nonce)
    return jsonify(200)
