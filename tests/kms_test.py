import datetime

import bencodex

from world_boss.app.kms import signer
from world_boss.app.models import Transaction


def test_address():
    assert signer.address == "0xCFCd6565287314FF70e4C4CF309dB701C43eA5bD"


def test_transfer_assets(fx_session):
    time_stamp = datetime.datetime(2022, 12, 31, tzinfo=datetime.timezone.utc)
    nonce = 1
    recipient = {
        "recipient": signer.address,
        "amount": {"quantity": 10, "decimalPlaces": 18, "ticker": "CRYSTAL"},
    }
    signer.transfer_assets(
        time_stamp,
        nonce,
        [recipient],
        "test",
        "http://9c-internal-rpc-1.nine-chronicles.com/graphql",
    )
    transaction = fx_session.query(Transaction).first()
    assert transaction.nonce == 1
    payload = transaction.payload
    tx = bencodex.loads(bytes.fromhex(payload))
    assert tx[b"n"] == 1
    action = tx[b"a"][0]
    assert action["type_id"] == "transfer_assets"
    plain_value = action["values"]
    assert plain_value["memo"] == "test"
    assert len(plain_value["recipients"]) == 1
