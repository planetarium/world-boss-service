import csv
import datetime
from io import StringIO
from typing import cast

from flask import Blueprint, Response, jsonify, request
from sqlalchemy import func

from world_boss.app.models import Transaction
from world_boss.app.orm import db
from world_boss.app.raid import get_next_tx_nonce, get_raid_rewards, row_to_recipient
from world_boss.app.slack import client
from world_boss.app.stubs import Recipient
from world_boss.app.tasks import count_users, get_ranking_rewards, sign_transfer_assets

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


@api.post("/raid/prepare")
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
    recipient_map: dict[int, list[Recipient]] = {}
    start_ranking: int
    last_ranking: int
    # raid_id,ranking,agent_address,avatar_address,amount,ticker,decimal_places,target_nonce
    for row in reader:
        nonce = int(row[7])
        recipient = row_to_recipient(row)
        # first row by each nonce
        if not recipient_map.get(nonce):
            recipient_map[nonce] = []
        recipient_map[nonce].append(recipient)
    for k in recipient_map:
        assert len(recipient_map[k]) <= 100
    sign_transfer_assets.delay(recipient_map, time_stamp)
    return jsonify(200)


@api.post("/nonce")
def next_tx_nonce():
    channel_id = request.values.get("channel_id")
    nonce = get_next_tx_nonce()
    client.chat_postMessage(
        channel=channel_id,
        text=f"next tx nonce: {nonce}",
    )
    return jsonify(200)
