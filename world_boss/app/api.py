import csv
from io import StringIO
from typing import Annotated, cast

import httpx
from celery import chord
from fastapi import APIRouter, Depends, Form, Request
from sqlalchemy import text
from sqlalchemy.orm import Session
from starlette.responses import JSONResponse, Response

from world_boss.app.config import config
from world_boss.app.enums import NetworkType
from world_boss.app.kms import signer
from world_boss.app.models import Transaction
from world_boss.app.orm import SessionLocal
from world_boss.app.raid import (
    get_currencies,
    get_next_tx_nonce,
    get_raid_rewards,
    list_tx_nonce,
    row_to_recipient,
)
from world_boss.app.schemas import WorldBossRewardSchema
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

api = APIRouter()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@api.get("/ping")
def pong(db: Session = Depends(get_db)) -> JSONResponse:
    try:
        db.execute(text("select 1"))
        status_code = 200
        message = "pong"
    except Exception as e:
        status_code = 503
        message = "database connection failed"
    return JSONResponse(message, status_code)


@api.get("/raid/{raid_id}/{avatar_address}/rewards")
def raid_rewards(
    response: Response,
    raid_id: int,
    avatar_address: str,
    db: Session = Depends(get_db),
) -> WorldBossRewardSchema:
    return get_raid_rewards(raid_id, avatar_address, db, response)


@api.post("/raid/list/count")
@slack_auth
async def count_total_users(
    request: Request, channel_id: Annotated[str, Form()], text: Annotated[int, Form()]
):
    task = count_users.delay(channel_id, text)
    return JSONResponse(task.id)


@api.post("/raid/rewards/list")
@slack_auth
async def generate_ranking_rewards_csv(
    request: Request, text: Annotated[str, Form()], channel_id: Annotated[str, Form()]
):
    values = text.split()
    raid_id, total_users, nonce = [int(v) for v in values]
    task = get_ranking_rewards.delay(channel_id, raid_id, total_users, nonce, 100)
    return JSONResponse(task.id)


@api.post("/raid/prepare")
@slack_auth
async def prepare_transfer_assets(
    request: Request,
    text: Annotated[str, Form()],
    channel_id: Annotated[str, Form()],
    db: Session = Depends(get_db),
) -> JSONResponse:
    link, time_stamp = text.split()
    file_id = link.split("/")[5]
    res = client.files_info(file=file_id)
    data = cast(dict, res.data)
    file = data["file"]
    content = httpx.get(
        file["url_private"], headers={"Authorization": "Bearer %s" % client.token}
    ).content.decode()
    stream = StringIO(content)
    has_header = csv.Sniffer().has_header(content)
    reader = csv.reader(stream)
    if has_header:
        next(reader, None)
    # nonce : recipients for transfer_assets tx
    recipient_map: dict[int, list[Recipient]] = {}
    max_nonce = get_next_tx_nonce(db) - 1
    exist_nonce = list_tx_nonce(db)
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
    url = config.headless_url
    task = chord(
        sign_transfer_assets.s(
            time_stamp,
            int(nonce),
            recipient_map[nonce],
            memo,
            url,
            max_nonce,
            exist_nonce,
        )
        for nonce in recipient_map
    )(insert_world_boss_rewards.si(rows, signer.address))
    return JSONResponse(task.id)


@api.post("/nonce", status_code=200)
@slack_auth
async def next_tx_nonce(
    request: Request, channel_id: Annotated[str, Form()], db: Session = Depends(get_db)
):
    nonce = get_next_tx_nonce(db)
    client.chat_postMessage(
        channel=channel_id,
        text=f"next tx nonce: {nonce}",
    )
    return JSONResponse(200)


@api.post("/prepare-reward-assets")
@slack_auth
async def prepare_reward_assets(
    request: Request, channel_id: Annotated[str, Form()], text: Annotated[int, Form()]
):
    task = upload_prepare_reward_assets.delay(channel_id, text)
    return JSONResponse(task.id)


@api.post("/stage-transaction")
@slack_auth
async def stage_transactions(
    request: Request,
    channel_id: Annotated[str, Form()],
    text: Annotated[str, Form()],
    db: Session = Depends(get_db),
):
    nonce_list = (
        db.query(Transaction.nonce)
        .filter_by(signer=signer.address, tx_result=None)
        .all()
    )
    network_type = NetworkType.INTERNAL
    if text.lower() == "main":
        network_type = NetworkType.MAIN
    headless_urls = [config.headless_url]
    task = chord(
        stage_transaction.s(headless_url, nonce)
        for headless_url in headless_urls
        for nonce, in nonce_list
    )(send_slack_message.si(channel_id, f"stage {len(nonce_list)} transactions"))
    return JSONResponse(task.id)


@api.post("/transaction-result")
@slack_auth
async def transaction_result(
    request: Request,
    channel_id: Annotated[str, Form()],
    db: Session = Depends(get_db),
):
    tx_ids = db.query(Transaction.tx_id).filter_by(tx_result=None)
    url = config.headless_url
    task = chord(query_tx_result.s(url, str(tx_id)) for tx_id, in tx_ids)(
        upload_tx_result.s(channel_id)
    )
    return JSONResponse(task.id)


@api.post("/balance")
@slack_auth
async def check_balance(
    request: Request, channel_id: Annotated[str, Form()], db: Session = Depends(get_db)
):
    currencies = get_currencies(db)
    url = config.headless_url
    task = chord(check_signer_balance.s(url, currency) for currency in currencies)(
        upload_balance_result.s(channel_id)
    )
    return JSONResponse(task.id)
