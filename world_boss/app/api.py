import csv
from io import StringIO
from typing import cast

from celery import chord
from flask import Blueprint, Response, jsonify, request

from world_boss.app.enums import NetworkType
from world_boss.app.kms import HEADLESS_URLS, MINER_URLS, signer
from world_boss.app.models import Transaction
from world_boss.app.orm import db
from world_boss.app.raid import (
    get_currencies,
    get_next_tx_nonce,
    get_raid_rewards,
    row_to_recipient,
)
from world_boss.app.slack import client, slack_auth
from world_boss.app.stubs import Recipient
from world_boss.app.tasks import (
    check_signer_balance,
    count_users,
    get_ranking_rewards,
    insert_world_boss_rewards,
    query_tx_result,
    send_slack_message,
    sign_transfer_assets,
    stage_transaction,
    upload_balance_result,
    upload_prepare_reward_assets,
    upload_tx_result,
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
    task = count_users.delay(channel_id, raid_id)
    return jsonify({"task_id": task.id})


@api.post("/raid/rewards/list")
@slack_auth
def generate_ranking_rewards_csv():
    values = request.values.get("text").split()
    raid_id, total_users, nonce = [int(v) for v in values]
    channel_id = request.values.get("channel_id")
    task = get_ranking_rewards.delay(channel_id, raid_id, total_users, nonce)
    return jsonify({"task_id": task.id})


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
    # nonce : recipients for transfer_assets tx
    recipient_map: dict[int, list[Recipient]] = {}
    max_nonce = get_next_tx_nonce() - 1
    rows = [row for row in reader]
    # raid_id,ranking,agent_address,avatar_address,amount,ticker,decimal_places,target_nonce
    for row in rows:
        nonce = int(row[7])
        recipient = row_to_recipient(row)

        # update recipient_map
        if not recipient_map.get(nonce):
            recipient_map[nonce] = []
        recipient_map[nonce].append(recipient)

    # sanity check
    for k in recipient_map:
        assert len(recipient_map[k]) <= 100
    # insert tables
    memo = "world boss ranking rewards by world boss signer"
    url = MINER_URLS[NetworkType.MAIN]
    task = chord(
        sign_transfer_assets.s(
            time_stamp, int(nonce), recipient_map[nonce], memo, url, max_nonce
        )
        for nonce in recipient_map
    )(insert_world_boss_rewards.si(rows))
    return jsonify({"task_id": task.id})


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
    task = upload_prepare_reward_assets.delay(channel_id, raid_id)
    return jsonify({"task_id": task.id})


@api.post("/stage-transaction")
@slack_auth
def stage_transactions():
    channel_id = request.values.get("channel_id")
    network = request.values.get("text")
    nonce_list = (
        db.session.query(Transaction.nonce)
        .filter_by(signer=signer.address, tx_result=None)
        .all()
    )
    network_type = NetworkType.INTERNAL
    if network.lower() == "main":
        network_type = NetworkType.MAIN
    headless_urls = HEADLESS_URLS[network_type]
    task = chord(
        stage_transaction.s(headless_url, nonce)
        for headless_url in headless_urls
        for nonce, in nonce_list
    )(send_slack_message.si(channel_id, f"stage {len(nonce_list)} transactions"))
    return jsonify({"task_id": task.id})


@api.post("/transaction-result")
@slack_auth
def transaction_result():
    channel_id = request.values.get("channel_id")
    network = request.values.get("text")
    tx_ids = db.session.query(Transaction.tx_id).filter_by(tx_result=None)
    network_type = NetworkType.INTERNAL
    if network.lower() == "main":
        network_type = NetworkType.MAIN
    url = MINER_URLS[network_type]
    task = chord(query_tx_result.s(url, str(tx_id)) for tx_id, in tx_ids)(
        upload_tx_result.s(channel_id)
    )
    return jsonify({"task_id": task.id})


@api.post("/balance")
@slack_auth
def check_balance():
    channel_id = request.values.get("channel_id")
    currencies = get_currencies()
    url = MINER_URLS[NetworkType.MAIN]
    task = chord(check_signer_balance.s(url, currency) for currency in currencies)(
        upload_balance_result.s(channel_id)
    )
    return jsonify({"task_id": task.id})
