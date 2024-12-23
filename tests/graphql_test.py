from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from celery.result import AsyncResult
from pytest_httpx import HTTPXMock

from world_boss.app.config import config
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
        url=config.data_provider_url,
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


@pytest.mark.parametrize("size", [100, 50])
def test_generate_ranking_rewards_csv(
    fx_test_client,
    celery_session_worker,
    httpx_mock: HTTPXMock,
    fx_ranking_rewards,
    size: int,
):
    requested_rewards = [
        {
            "raider": {
                "address": "01A0b412721b00bFb5D619378F8ab4E4a97646Ca",
                "ranking": 101,
            },
            "rewards": fx_ranking_rewards,
        },
    ]
    httpx_mock.add_response(
        method="POST",
        url=config.data_provider_url,
        json={"data": {"worldBossRankingRewards": requested_rewards}},
    )

    httpx_mock.add_response(
        method="POST",
        url=config.headless_url,
        json={
            "data": {
                "stateQuery": {
                    "arg01A0b412721b00bFb5D619378F8ab4E4a97646Ca": {
                        "agentAddress": "0x9EBD1b4F9DbB851BccEa0CFF32926d81eDf6De52",
                    },
                }
            }
        },
    )

    query = f'mutation {{ generateRankingRewardsCsv(seasonId: 1, totalUsers: 1, startNonce: 1, password: "{config.graphql_password}", size: {size}) }}'
    with patch("world_boss.app.tasks.client.files_upload_v2") as m:
        req = fx_test_client.post("/graphql", json={"query": query})
        assert req.status_code == 200
        task_id = req.json()["data"]["generateRankingRewardsCsv"]
        task: AsyncResult = AsyncResult(task_id)
        task.get(timeout=30)
        assert task.state == "SUCCESS"
        m.assert_called_once()
        # skip check file. because file is temp file.
        kwargs = m.call_args.kwargs
        assert kwargs["file"]
        assert kwargs["channels"] == config.slack_channel_id
        assert kwargs["title"] == f"world_boss_1_1_1_{size}_result"
        assert kwargs["filename"] == f"world_boss_1_1_1_{size}_result.csv"


@pytest.mark.parametrize("has_header", [True, False])
def test_prepare_transfer_assets(
    fx_test_client,
    celery_session_worker,
    fx_session,
    httpx_mock: HTTPXMock,
    has_header: bool,
):
    assert not fx_session.query(Transaction).first()
    assert not fx_session.query(WorldBossReward).first()
    assert not fx_session.query(WorldBossRewardAmount).first()
    header = "raid_id,ranking,agent_address,avatar_address,amount,ticker,decimal_places,target_nonce\n"
    content = """3,25,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,150000,CRYSTAL,18,175
    3,25,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,560,RUNESTONE_FENRIR1,0,175
    3,25,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,150,RUNESTONE_FENRIR2,0,175
    3,25,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,40,RUNESTONE_FENRIR3,0,175
    3,26,0x1774cd5d2C1C0f72AA75E9381889a1a554797a4c,1F8d5e0D201B7232cE3BC8d630d09E3F9107CceE,150000,CRYSTAL,18,176
    3,26,0x1774cd5d2C1C0f72AA75E9381889a1a554797a4c,1F8d5e0D201B7232cE3BC8d630d09E3F9107CceE,560,RUNESTONE_FENRIR1,0,176
    3,26,0x1774cd5d2C1C0f72AA75E9381889a1a554797a4c,1F8d5e0D201B7232cE3BC8d630d09E3F9107CceE,150,RUNESTONE_FENRIR2,0,176
    3,26,0x1774cd5d2C1C0f72AA75E9381889a1a554797a4c,1F8d5e0D201B7232cE3BC8d630d09E3F9107CceE,40,RUNESTONE_FENRIR3,0,176"""

    expected = [
        {
            "nonce": 175,
            "ranking": 25,
            "agent_address": "0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4",
            "avatar_address": "5b65f5D0e23383FA18d74A62FbEa383c7D11F29d",
        },
        {
            "nonce": 176,
            "ranking": 26,
            "agent_address": "0x1774cd5d2C1C0f72AA75E9381889a1a554797a4c",
            "avatar_address": "1F8d5e0D201B7232cE3BC8d630d09E3F9107CceE",
        },
    ]
    reward_amounts = [
        {
            "ticker": "CRYSTAL",
            "decimal_places": 18,
            "amount": 150000,
        },
        {
            "ticker": "RUNESTONE_FENRIR1",
            "decimal_places": 0,
            "amount": 560,
        },
        {
            "ticker": "RUNESTONE_FENRIR2",
            "decimal_places": 0,
            "amount": 150,
        },
        {
            "ticker": "RUNESTONE_FENRIR3",
            "decimal_places": 0,
            "amount": 40,
        },
    ]

    private_url = "https://planetariumhq.slack.com/private/files/1/2/test.csv"
    mocked_response = MagicMock()
    mocked_response.data = {
        "file": {"url_private": private_url},
    }
    httpx_mock.add_response(
        method="GET",
        url=private_url,
        content=(header + content).encode() if has_header else content.encode(),
    )

    query = f'mutation {{ prepareTransferAssets(link: "https://planetariumhq.slack.com/files/1/2/test.csv", timeStamp: "2022-12-31", password:"{config.graphql_password}") }}'

    with patch(
        "world_boss.app.api.client.files_info", return_value=mocked_response
    ) as m:
        req = fx_test_client.post("/graphql", json={"query": query})
        assert req.status_code == 200
        task_id = req.json()["data"]["prepareTransferAssets"]
        task: AsyncResult = AsyncResult(task_id)
        task.get(timeout=30)
        assert task.state == "SUCCESS"
        m.assert_called_once_with(file="2")
        assert fx_session.query(Transaction).count() == 2
        assert fx_session.query(WorldBossReward).count() == 2
        assert fx_session.query(WorldBossRewardAmount).count() == 8
        for i, tx in enumerate(
            fx_session.query(Transaction).order_by(Transaction.nonce)
        ):
            assert tx.nonce == expected[i]["nonce"]
            assert tx.tx_result is None
            assert len(tx.amounts) == 4

            world_boss_reward = tx.amounts[0].reward
            assert world_boss_reward.raid_id == 3
            assert world_boss_reward.ranking == expected[i]["ranking"]
            assert world_boss_reward.agent_address == expected[i]["agent_address"]
            assert world_boss_reward.avatar_address == expected[i]["avatar_address"]

            for v, world_boss_reward_amount in enumerate(tx.amounts):
                assert world_boss_reward_amount.ticker == reward_amounts[v]["ticker"]
                assert (
                    world_boss_reward_amount.decimal_places
                    == reward_amounts[v]["decimal_places"]
                )
                assert world_boss_reward_amount.amount == reward_amounts[v]["amount"]


def test_prepare_reward_assets(fx_test_client, celery_session_worker, fx_session):
    result = []
    assets = [
        {"decimalPlaces": 18, "ticker": "CRYSTAL", "quantity": 109380000},
        {"decimalPlaces": 0, "ticker": "RUNESTONE_FENRIR1", "quantity": 406545},
        {"decimalPlaces": 0, "ticker": "RUNESTONE_FENRIR2", "quantity": 111715},
        {"decimalPlaces": 0, "ticker": "RUNESTONE_FENRIR3", "quantity": 23890},
        {"decimalPlaces": 0, "ticker": "Item_NT_500000", "quantity": 300},
    ]
    reward = WorldBossReward()
    reward.avatar_address = "avatar_address"
    reward.agent_address = "agent_address"
    reward.raid_id = 3
    reward.ranking = 1

    for i, asset in enumerate(assets):
        transaction = Transaction()
        tx_id = i
        transaction.tx_id = tx_id
        transaction.signer = "signer"
        transaction.payload = "payload"
        transaction.nonce = i
        reward_amount = WorldBossRewardAmount()
        reward_amount.amount = asset["quantity"]
        reward_amount.ticker = asset["ticker"]
        reward_amount.decimal_places = asset["decimalPlaces"]
        reward_amount.reward = reward
        reward_amount.transaction = transaction
        fx_session.add(transaction)
        result.append(reward_amount)
    fx_session.commit()
    query = f'mutation {{ prepareRewardAssets(seasonId: 3, password: "{config.graphql_password}") }}'
    with patch("world_boss.app.tasks.client.chat_postMessage") as m:
        req = fx_test_client.post("/graphql", json={"query": query})
        assert req.status_code == 200
        task_id = req.json()["data"]["prepareRewardAssets"]
        task = AsyncResult(task_id)
        task.get(timeout=30)
        assert task.state == "SUCCESS"

        m.assert_called_once_with(
            channel=config.slack_channel_id,
            text="world boss season 3 prepareRewardAssets\n```plain_value:{'type_id': 'prepare_reward_assets', 'values': {'r': b'%1\\xe5\\xe0l\\xbd\\x11\\xafT\\xf9\\x8d9W\\x89\\x90qo\\xfc}\\xba', 'a': [[{'decimalPlaces': b'\\x12', 'minters': None, 'ticker': 'FAV__CRYSTAL'}, 109380000000000000000000000], [{'decimalPlaces': b'\\x00', 'minters': None, 'ticker': 'Item_NT_500000'}, 300], [{'decimalPlaces': b'\\x00', 'minters': None, 'ticker': 'FAV__RUNESTONE_FENRIR1'}, 406545], [{'decimalPlaces': b'\\x00', 'minters': None, 'ticker': 'FAV__RUNESTONE_FENRIR2'}, 111715], [{'decimalPlaces': b'\\x00', 'minters': None, 'ticker': 'FAV__RUNESTONE_FENRIR3'}, 23890]]}}\n\n6475373a747970655f69647532313a707265706172655f7265776172645f61737365747375363a76616c7565736475313a616c6c647531333a646563696d616c506c61636573313a1275373a6d696e746572736e75363a7469636b65727531323a4641565f5f4352595354414c656931303933383030303030303030303030303030303030303030303065656c647531333a646563696d616c506c61636573313a0075373a6d696e746572736e75363a7469636b65727531343a4974656d5f4e545f353030303030656933303065656c647531333a646563696d616c506c61636573313a0075373a6d696e746572736e75363a7469636b65727532323a4641565f5f52554e4553544f4e455f46454e52495231656934303635343565656c647531333a646563696d616c506c61636573313a0075373a6d696e746572736e75363a7469636b65727532323a4641565f5f52554e4553544f4e455f46454e52495232656931313137313565656c647531333a646563696d616c506c61636573313a0075373a6d696e746572736e75363a7469636b65727532323a4641565f5f52554e4553544f4e455f46454e524952336569323338393065656575313a7232303a2531e5e06cbd11af54f98d39578990716ffc7dba6565```",
        )


def test_stage_transactions(
    fx_test_client,
    celery_session_worker,
    fx_session,
    fx_transactions,
):
    for tx in fx_transactions:
        fx_session.add(tx)
    fx_session.commit()
    query = f'mutation {{ stageTransactions(password: "{config.graphql_password}") }}'
    with patch("world_boss.app.graphql.stage_transactions_with_countdown.delay") as m:
        req = fx_test_client.post("/graphql", json={"query": query})
        assert req.status_code == 200
        req.json()["data"]["stageTransactions"]
        m.assert_called_once_with(config.headless_url, [1, 2])


def test_transaction_result(
    fx_test_client,
    fx_session,
    celery_session_worker,
):
    for nonce, tx_id, payload in [
        (
            1,
            "a9c9444bd50b3164b5c251315960272ae1f42f7b2d5b95948a78c608424bbcb2",
            "payload_1",
        ),
        (
            2,
            "db4b916c5c821cbf90356694f231c9f6a6858b67231799dc9ee2d9f2946c4310",
            "payload_2",
        ),
    ]:
        transaction = Transaction()
        transaction.tx_id = tx_id
        transaction.nonce = nonce
        transaction.payload = payload
        transaction.signer = "0xCFCd6565287314FF70e4C4CF309dB701C43eA5bD"
        fx_session.add(transaction)
    fx_session.commit()
    query = f'mutation {{ transactionResult(password: "{config.graphql_password}") }}'
    with patch("world_boss.app.tasks.client.files_upload_v2") as m:
        req = fx_test_client.post("/graphql", json={"query": query})
        assert req.status_code == 200
        task_id = req.json()["data"]["transactionResult"]
        task: AsyncResult = AsyncResult(task_id)
        task.get(timeout=30)
        assert task.state == "SUCCESS"
        m.assert_called_once()
        kwargs = m.call_args.kwargs
        assert kwargs["file"]
        assert kwargs["channels"] == config.slack_channel_id
        assert kwargs["title"] == "world_boss_tx_result"
        assert "world_boss_tx_result" in kwargs["filename"]
        for tx in fx_session.query(Transaction):
            assert tx.tx_result == "INCLUDED"
