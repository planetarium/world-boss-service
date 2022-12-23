import datetime
from typing import List

import bencodex
import pytest
from pytest_httpx import HTTPXMock

from world_boss.app.enums import NetworkType
from world_boss.app.kms import HEADLESS_URLS, MINER_URLS, signer
from world_boss.app.models import Transaction
from world_boss.app.stubs import AmountDictionary, Recipient


def test_address():
    assert signer.address == "0x2531e5e06cBD11aF54f98D39578990716fFC7dBa"


def test_transfer_assets(fx_session) -> None:
    time_stamp = datetime.datetime(2022, 12, 31, tzinfo=datetime.timezone.utc)
    nonce = 2
    recipient: Recipient = {
        "recipient": signer.address,
        "amount": {
            "quantity": 10,
            "decimalPlaces": 18,
            "ticker": "CRYSTAL",
        },
    }
    result = signer.transfer_assets(
        time_stamp,
        nonce,
        [recipient],
        "test",
        "http://9c-internal-rpc-1.nine-chronicles.com/graphql",
    )
    transaction = fx_session.query(Transaction).first()
    assert result == transaction
    assert transaction.nonce == 2
    payload = transaction.payload
    tx = bencodex.loads(bytes.fromhex(payload))
    assert tx[b"n"] == 2
    action = tx[b"a"][0]
    assert action["type_id"] == "transfer_assets"
    plain_value = action["values"]
    assert plain_value["memo"] == "test"
    assert len(plain_value["recipients"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "network_type",
    [
        NetworkType.INTERNAL,
        NetworkType.MAIN,
    ],
)
async def test_stage_transactions_async(
    fx_session, httpx_mock: HTTPXMock, network_type: NetworkType
):
    for nonce in [1, 2, 3]:
        tx = Transaction()
        tx.tx_id = str(nonce)
        tx.nonce = nonce
        tx.signer = "signer"
        tx.payload = "payload"
        fx_session.add(tx)
    fx_session.flush()
    urls = HEADLESS_URLS[network_type]
    for url in urls:
        httpx_mock.add_response(
            url=url,
            method="POST",
            json={
                "data": {
                    "stageTransaction": 1,
                }
            },
        )
    await signer.stage_transactions_async(network_type)
    assert len(httpx_mock.get_requests()) == len(urls) * 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "network_type",
    [
        NetworkType.INTERNAL,
        NetworkType.MAIN,
    ],
)
async def test_check_transaction_status_async(
    fx_session, httpx_mock: HTTPXMock, network_type: NetworkType
):
    for nonce in [1, 2, 3]:
        tx = Transaction()
        tx.tx_id = str(nonce)
        tx.nonce = nonce
        tx.signer = "signer"
        tx.payload = "payload"
        fx_session.add(tx)
    fx_session.flush()
    httpx_mock.add_response(
        method="POST",
        url=MINER_URLS[network_type],
        json={"data": {"transaction": {"transactionResult": {"txStatus": "SUCCESS"}}}},
    )
    await signer.check_transaction_status_async(network_type)
    assert len(httpx_mock.get_requests()) == 3
    transactions = fx_session.query(Transaction)
    for transaction in transactions:
        assert transaction.tx_result == "SUCCESS"


@pytest.mark.parametrize(
    "network_type",
    [
        NetworkType.INTERNAL,
        NetworkType.MAIN,
    ],
)
def test_prepare_reward_assets(fx_app, network_type: NetworkType):
    headless_url = MINER_URLS[network_type]
    assets: List[AmountDictionary] = [
        {"decimalPlaces": 18, "ticker": "CRYSTAL", "quantity": 109380000},
        {"decimalPlaces": 0, "ticker": "RUNESTONE_FENRIR1", "quantity": 406545},
        {"decimalPlaces": 0, "ticker": "RUNESTONE_FENRIR2", "quantity": 111715},
        {"decimalPlaces": 0, "ticker": "RUNESTONE_FENRIR3", "quantity": 23890},
    ]
    result = signer.prepare_reward_assets(headless_url, assets)
    assert (
        result
        == "6475373a747970655f69647532313a707265706172655f7265776172645f61737365747375363a76616c7565736475313a616c6c647531333a646563696d616c506c61636573313a1275373a6d696e746572736e75363a7469636b657275373a4352595354414c656931303933383030303030303030303030303030303030303030303065656c647531333a646563696d616c506c61636573313a0075373a6d696e746572736e75363a7469636b65727531373a52554e4553544f4e455f46454e52495231656934303635343565656c647531333a646563696d616c506c61636573313a0075373a6d696e746572736e75363a7469636b65727531373a52554e4553544f4e455f46454e52495232656931313137313565656c647531333a646563696d616c506c61636573313a0075373a6d696e746572736e75363a7469636b65727531373a52554e4553544f4e455f46454e524952336569323338393065656575313a7232303a2531e5e06cbd11af54f98d39578990716ffc7dba6565"
    )
