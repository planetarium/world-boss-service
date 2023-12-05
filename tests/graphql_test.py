from decimal import Decimal
from unittest.mock import patch

import pytest
from pytest_httpx import HTTPXMock

from world_boss.app.data_provider import DATA_PROVIDER_URLS
from world_boss.app.enums import NetworkType
from world_boss.app.models import Transaction, WorldBossReward, WorldBossRewardAmount


@pytest.fixture()
def non_mocked_hosts() -> list:
    return ["testserver"]


def test_next_tx_nonce(fx_session, fx_test_client):
    query = "query { nextTxNonce }"
    resp = fx_test_client.post("/graphql", json={"query": query})
    result = resp.json()
    assert result["data"]["nextTxNonce"] == 1


def test_count_total_users(fx_test_client, httpx_mock: HTTPXMock):
    httpx_mock.add_response(
        method="POST",
        url=DATA_PROVIDER_URLS[NetworkType.MAIN],
        json={"data": {"worldBossTotalUsers": 100}},
    )
    query = "query { countTotalUsers(seasonId: 1) }"
    req = fx_test_client.post("/graphql", json={"query": query})
    assert req.status_code == 200
    result = req.json()
    assert result["data"]["countTotalUsers"] == 100


def test_check_balance(fx_session, fx_test_client):
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
    expected = ["1 CRYSTAL", "2 RUNE_FENRIR1"]
    query = "query { checkBalance }"
    with patch(
        "world_boss.app.graphql.signer.query_balance",
        side_effect=expected,
    ) as m:
        req = fx_test_client.post("/graphql", json={"query": query})
        assert req.status_code == 200
        result = req.json()
        assert m.call_count == 2
        assert result["data"]["checkBalance"] == expected
