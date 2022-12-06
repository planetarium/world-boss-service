import datetime

import bencodex
import pytest

from world_boss.app.enums import NetworkType
from world_boss.app.kms import signer, HEADLESS_URLS
from world_boss.app.models import Transaction


def test_address():
    assert signer.address == "0x2531e5e06cBD11aF54f98D39578990716fFC7dBa"


def test_transfer_assets(fx_session):
    time_stamp = datetime.datetime(2022, 12, 31, tzinfo=datetime.timezone.utc)
    nonce = 2
    recipient = {
        "recipient": signer.address,
        "amount": {"quantity": 10, "decimalPlaces": 18, "ticker": "CRYSTAL"},
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


@pytest.mark.parametrize(
    "network_type",
    [
        NetworkType.INTERNAL,
        NetworkType.MAIN,
    ],
)
def test_stage_transactions(fx_session, requests_mock, network_type: NetworkType):
    urls = HEADLESS_URLS[network_type]
    for nonce in [1, 2, 3]:
        tx = Transaction()
        tx.tx_id = str(nonce)
        tx.nonce = nonce
        tx.signer = "signer"
        tx.payload = "payload"
        fx_session.add(tx)
    fx_session.flush()
    for url in urls:
        requests_mock.post(
            url,
            json={
                "data": {
                    "stageTransaction": 1,
                }
            },
        )
    signer.stage_transactions(network_type)
    assert len(requests_mock.request_history) == len(urls) * 3


@pytest.mark.parametrize(
    "network_type",
    [
        NetworkType.INTERNAL,
        NetworkType.MAIN,
    ],
)
def test_check_transaction_status(fx_session, requests_mock, network_type: NetworkType):
    urls = HEADLESS_URLS[network_type]
    for nonce in [1, 2, 3]:
        tx = Transaction()
        tx.tx_id = str(nonce)
        tx.nonce = nonce
        tx.signer = "signer"
        tx.payload = "payload"
        fx_session.add(tx)
    fx_session.flush()
    for url in urls:
        requests_mock.post(
            url,
            json={
                "data": {"transaction": {"transactionResult": {"txStatus": "SUCCESS"}}}
            },
        )
        signer.check_transaction_status(network_type)
    transactions = fx_session.query(Transaction).all()
    for transaction in transactions:
        assert transaction.tx_result == "SUCCESS"
