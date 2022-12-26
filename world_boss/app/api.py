import csv
from io import StringIO
from typing import cast

from flask import Blueprint, Response, jsonify, request

from world_boss.app.models import Transaction
from world_boss.app.orm import db
from world_boss.app.raid import get_next_tx_nonce, get_raid_rewards
from world_boss.app.slack import client, slack_auth
from world_boss.app.tasks import (
    check_tx_result,
    count_users,
    get_ranking_rewards,
    prepare_world_boss_ranking_rewards,
    stage_transactions,
    upload_prepare_reward_assets,
)

api = Blueprint("api", __name__)


@api.route("/ping")
def pong() -> str:
    return "pong"


@api.route("/raid/<raid_id>/<avatar_address>/rewards", methods=["GET"])
def raid_rewards(raid_id: int, avatar_address: str):
    return get_raid_rewards(raid_id, avatar_address)


@api.post("/raid/list/count")
@slack_auth
def count_total_users():
    raid_id = request.values.get("text", type=int)
    channel_id = request.values.get("channel_id")
    count_users.delay(channel_id, raid_id)
    return jsonify(200)


@api.post("/raid/rewards/list")
@slack_auth
def generate_ranking_rewards_csv():
    values = request.values.get("text").split()
    raid_id, total_users, nonce = [int(v) for v in values]
    channel_id = request.values.get("channel_id")
    get_ranking_rewards.delay(channel_id, raid_id, total_users, nonce)
    return jsonify(200)


@api.post("/raid/prepare")
@slack_auth
def prepare_transfer_assets() -> Response:
    link, time_stamp = request.values.get("text", "").split()
    file_id = link.split("/")[5]
    res = client.files_info(file=file_id)
    data = cast(dict, res.data)
    content = data["content"]
    stream = StringIO(content)
    has_header = csv.Sniffer().has_header(content)
    reader = csv.reader(stream)
    if has_header:
        next(reader, None)
    prepare_world_boss_ranking_rewards.delay([row for row in reader], time_stamp)
    return jsonify(200)


@api.post("/nonce")
@slack_auth
def next_tx_nonce():
    channel_id = request.values.get("channel_id")
    nonce = get_next_tx_nonce()
    client.chat_postMessage(
        channel=channel_id,
        text=f"next tx nonce: {nonce}",
    )
    return jsonify(200)


@api.post("/prepare-reward-assets")
@slack_auth
def prepare_reward_assets():
    channel_id = request.values.get("channel_id")
    raid_id = request.values.get("text", type=int)
    upload_prepare_reward_assets.delay(channel_id, raid_id)
    return jsonify(200)


@api.post("/stage-transaction")
@slack_auth
def stage_transaction():
    channel_id = request.values.get("channel_id")
    network = request.values.get("text")
    stage_transactions.delay(channel_id, network)
    return jsonify(200)


@api.post("/transaction-result")
@slack_auth
def transaction_result():
    channel_id = request.values.get("channel_id")
    network = request.values.get("text")
    tx_ids = db.session.query(Transaction.tx_id).all()
    check_tx_result.delay(channel_id, [tx_id for tx_id, in tx_ids], network)
    return jsonify(200)
