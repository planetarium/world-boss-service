import datetime
from decimal import Decimal

import bencodex
import pytest
from gql.transport.exceptions import TransportQueryError

from world_boss.app.config import config
from world_boss.app.enums import NetworkType
from world_boss.app.kms import signer
from world_boss.app.models import Transaction, WorldBossReward, WorldBossRewardAmount
from world_boss.app.raid import get_currencies
from world_boss.app.stubs import Recipient


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
        fx_session,
    )
    transaction = fx_session.query(Transaction).first()
    assert result == transaction
    assert transaction.nonce == 2
    payload = transaction.payload
    tx = bencodex.loads(bytes.fromhex(payload))
    assert tx[b"n"] == 2
    action = tx[b"a"][0]
    assert action["type_id"] == "transfer_assets3"
    plain_value = action["values"]
    assert plain_value["memo"] == "test"
    assert len(plain_value["recipients"]) == 1


@pytest.mark.asyncio
async def test_stage_transactions_async(fx_session, fx_mainnet_transactions):
    fx_session.add_all(fx_mainnet_transactions)
    fx_session.flush()
    with pytest.raises(TransportQueryError):
        await signer.stage_transactions_async(NetworkType.INTERNAL, fx_session)


@pytest.mark.asyncio
async def test_check_transaction_status_async(fx_session, fx_mainnet_transactions):
    assert fx_session.query(Transaction).count() == 0
    fx_session.add_all(fx_mainnet_transactions)
    fx_session.flush()
    await signer.check_transaction_status_async(NetworkType.MAIN, fx_session)
    transactions = fx_session.query(Transaction)
    for transaction in transactions:
        assert transaction.tx_result == "INCLUDED"


def test_stage_transaction(fx_session, fx_mainnet_transactions):
    tx = fx_mainnet_transactions[0]
    fx_session.add(tx)
    fx_session.flush()
    urls = [config.headless_url]
    for url in urls:
        with pytest.raises(TransportQueryError) as e:
            signer.stage_transaction(url, tx)
            assert tx.tx_id in str(e.value)


def test_query_transaction_result(fx_session, fx_mainnet_transactions):
    tx = fx_mainnet_transactions[0]
    fx_session.add(tx)
    fx_session.flush()
    url = config.headless_url
    signer.query_transaction_result(url, tx.tx_id, fx_session)
    transaction = fx_session.query(Transaction).one()
    assert transaction.tx_result == "INCLUDED"


def test_query_balance(fx_session):
    url = config.headless_url
    reward = WorldBossReward()
    reward.avatar_address = "avatar_address"
    reward.agent_address = "agent_address"
    reward.raid_id = 1
    reward.ranking = 1
    i = 1
    for ticker, decimal_places in [("CRYSTAL", 18), ("RUNE_FENRIR1", 0)]:
        transaction = Transaction()
        transaction.tx_id = str(i)
        transaction.signer = "signer"
        transaction.payload = f"10 {ticker}"
        transaction.nonce = i
        reward_amount = WorldBossRewardAmount()
        reward_amount.amount = Decimal("10")
        reward_amount.ticker = ticker
        reward_amount.decimal_places = decimal_places
        reward_amount.reward = reward
        reward_amount.transaction = transaction
        fx_session.add(reward_amount)
        i += 1
    fx_session.commit()
    currencies = get_currencies(fx_session)
    for currency in currencies:
        balance = signer.query_balance(url, currency)
        assert balance == f'0 {currency["ticker"]}'
