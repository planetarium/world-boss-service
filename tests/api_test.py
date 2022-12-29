import json
import time
import unittest
from datetime import timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from celery.result import AsyncResult
from pytest_httpx import HTTPXMock

from world_boss.app.cache import cache_exists, set_to_cache
from world_boss.app.data_provider import DATA_PROVIDER_URLS
from world_boss.app.enums import NetworkType
from world_boss.app.kms import HEADLESS_URLS, MINER_URLS
from world_boss.app.models import Transaction, WorldBossReward, WorldBossRewardAmount


def test_raid_rewards_404(fx_test_client, redisdb, fx_session):
    req = fx_test_client.get("/raid/1/test/rewards")
    assert req.status_code == 404


@pytest.mark.parametrize(
    "caching",
    [
        True,
        False,
    ],
)
def test_raid_rewards(fx_test_client, fx_session, redis_proc, caching: bool):
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
    raid_id = reward.raid_id
    avatar_address = reward.avatar_address
    if caching:
        cache_key = f"raid_rewards_{avatar_address}_{raid_id}_json"
        set_to_cache(cache_key, json.dumps(reward.as_dict()), timedelta(seconds=1))
    req = fx_test_client.get(f"/raid/{raid_id}/{avatar_address}/rewards")
    assert req.status_code == 200
    assert req.json == reward.as_dict()
    if caching:
        time.sleep(2)
        assert not cache_exists(cache_key)


def test_count_total_users(
    fx_test_client, celery_session_worker, httpx_mock: HTTPXMock
):
    httpx_mock.add_response(
        method="POST",
        url=DATA_PROVIDER_URLS[NetworkType.MAIN],
        json={"data": {"worldBossTotalUsers": 100}},
    )
    with unittest.mock.patch(
        "world_boss.app.tasks.client.chat_postMessage"
    ) as m, unittest.mock.patch(
        "world_boss.app.slack.verifier.is_valid_request", return_value=True
    ):
        req = fx_test_client.post(
            "/raid/list/count", data={"text": 1, "channel_id": "channel_id"}
        )
        assert req.status_code == 200
        task_id = req.json["task_id"]
        task: AsyncResult = AsyncResult(task_id)
        task.get(timeout=30)
        assert task.state == "SUCCESS"
        m.assert_called_once_with(
            channel="channel_id", text="world boss season 1 total users: 100"
        )


def test_generate_ranking_rewards_csv(
    fx_test_client, celery_session_worker, httpx_mock: HTTPXMock, fx_ranking_rewards
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
        url=DATA_PROVIDER_URLS[NetworkType.MAIN],
        json={"data": {"worldBossRankingRewards": requested_rewards}},
    )

    httpx_mock.add_response(
        method="POST",
        url=MINER_URLS[NetworkType.MAIN],
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

    with unittest.mock.patch(
        "world_boss.app.tasks.client.files_upload_v2"
    ) as m, unittest.mock.patch(
        "world_boss.app.slack.verifier.is_valid_request", return_value=True
    ):
        req = fx_test_client.post(
            "/raid/rewards/list", data={"text": "1 1 1", "channel_id": "channel_id"}
        )
        assert req.status_code == 200
        task_id = req.json["task_id"]
        task: AsyncResult = AsyncResult(task_id)
        task.get(timeout=30)
        assert task.state == "SUCCESS"
        m.assert_called_once()
        # skip check file. because file is temp file.
        kwargs = m.call_args.kwargs
        assert kwargs["file"]
        assert kwargs["channels"] == "channel_id"
        assert kwargs["title"] == f"world_boss_1_1_1_result"
        assert kwargs["filename"] == f"world_boss_1_1_1_result.csv"


@pytest.mark.parametrize("has_header", [True, False])
def test_prepare_transfer_assets(
    fx_test_client, celery_session_worker, fx_session, has_header: bool
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

    mocked_response = MagicMock()
    mocked_response.data = {
        "content": header + content if has_header else content,
    }
    with unittest.mock.patch(
        "world_boss.app.api.client.files_info", return_value=mocked_response
    ) as m, unittest.mock.patch(
        "world_boss.app.slack.verifier.is_valid_request", return_value=True
    ):
        req = fx_test_client.post(
            f"/raid/prepare",
            data={
                "text": "https://planetariumhq.slack.com/files/1/2/test.csv 2022-12-31",
                "channel_id": "channel_id",
            },
        )
        assert req.status_code == 200
        task_id = req.json["task_id"]
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


def test_next_tx_nonce(
    fx_test_client,
    fx_session,
):
    tx = Transaction()
    tx.nonce = 1
    tx.tx_id = "tx_id"
    tx.signer = "0xCFCd6565287314FF70e4C4CF309dB701C43eA5bD"
    tx.payload = "payload"
    fx_session.add(tx)
    fx_session.flush()
    with unittest.mock.patch(
        "world_boss.app.api.client.chat_postMessage"
    ) as m, unittest.mock.patch(
        "world_boss.app.slack.verifier.is_valid_request", return_value=True
    ):
        req = fx_test_client.post("/nonce", data={"channel_id": "channel_id"})
        assert req.status_code == 200
        assert req.json == 200
        m.assert_called_once_with(channel="channel_id", text="next tx nonce: 2")


def test_prepare_reward_assets(fx_test_client, celery_session_worker, fx_session):
    result = []
    assets = [
        {"decimalPlaces": 18, "ticker": "CRYSTAL", "quantity": 109380000},
        {"decimalPlaces": 0, "ticker": "RUNESTONE_FENRIR1", "quantity": 406545},
        {"decimalPlaces": 0, "ticker": "RUNESTONE_FENRIR2", "quantity": 111715},
        {"decimalPlaces": 0, "ticker": "RUNESTONE_FENRIR3", "quantity": 23890},
    ]
    reward = WorldBossReward()
    reward.avatar_address = "avatar_address"
    reward.agent_address = "agent_address"
    reward.raid_id = 3
    reward.ranking = 1

    for i, asset in enumerate(assets):
        reward_amount = WorldBossRewardAmount()
        reward_amount.amount = asset["quantity"]
        reward_amount.ticker = asset["ticker"]
        reward_amount.decimal_places = asset["decimalPlaces"]
        reward_amount.reward = reward
        tx_id = i
        reward_amount.tx_id = tx_id
        transaction = Transaction()
        transaction.tx_id = tx_id
        transaction.signer = "signer"
        transaction.payload = "payload"
        transaction.nonce = i
        fx_session.add(transaction)
        result.append(reward_amount)
    fx_session.commit()
    with unittest.mock.patch(
        "world_boss.app.tasks.client.chat_postMessage"
    ) as m, unittest.mock.patch(
        "world_boss.app.slack.verifier.is_valid_request", return_value=True
    ):
        req = fx_test_client.post(
            "/prepare-reward-assets", data={"channel_id": "channel_id", "text": "3"}
        )
        assert req.status_code == 200
        task_id = req.json["task_id"]
        task = AsyncResult(task_id)
        task.get(timeout=30)
        assert task.state == "SUCCESS"

        m.assert_called_once_with(
            channel="channel_id",
            text="world boss season 3 prepareRewardAssets\n```plain_value:{'type_id': 'prepare_reward_assets', 'values': {'a': "
            "[], 'r': "
            "b'%1\\xe5\\xe0l\\xbd\\x11\\xafT\\xf9\\x8d9W\\x89\\x90qo\\xfc}\\xba'}}\n"
            "\n"
            "6475373a747970655f69647532313a707265706172655f7265776172645f61737365747375363a76616c7565736475313a616c6575313a7232303a2531e5e06cbd11af54f98d39578990716ffc7dba6565```",
        )


@pytest.mark.parametrize("text", ["main", "internal"])
def test_stage_transactions(
    fx_test_client,
    celery_session_worker,
    fx_session,
    fx_transactions,
    httpx_mock: HTTPXMock,
    text: str,
):
    for tx in fx_transactions:
        fx_session.add(tx)
    fx_session.commit()
    network_type = NetworkType.MAIN if text.lower() == "main" else NetworkType.INTERNAL
    with unittest.mock.patch(
        "world_boss.app.tasks.signer.stage_transaction", return_value="tx_id"
    ) as m, unittest.mock.patch(
        "world_boss.app.tasks.client.chat_postMessage"
    ) as m2, unittest.mock.patch(
        "world_boss.app.slack.verifier.is_valid_request", return_value=True
    ):
        req = fx_test_client.post(
            "/stage-transaction", data={"channel_id": "channel_id", "text": text}
        )
        assert req.status_code == 200
        task_id = req.json["task_id"]
        task: AsyncResult = AsyncResult(task_id)
        task.get(timeout=30)
        assert m.call_count == len(HEADLESS_URLS[network_type]) * len(fx_transactions)
        m2.assert_called_once_with(
            channel="channel_id", text=f"stage {len(fx_transactions)} transactions"
        )


@pytest.mark.parametrize("text", ["main", "MAIN"])
def test_transaction_result(
    fx_test_client, fx_session, celery_session_worker, text: str
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
    with unittest.mock.patch(
        "world_boss.app.tasks.client.files_upload_v2"
    ) as m, unittest.mock.patch(
        "world_boss.app.slack.verifier.is_valid_request", return_value=True
    ):
        req = fx_test_client.post(
            "/transaction-result", data={"channel_id": "channel_id", "text": text}
        )
        assert req.status_code == 200
        task_id = req.json["task_id"]
        task: AsyncResult = AsyncResult(task_id)
        task.get(timeout=30)
        assert task.state == "SUCCESS"
        m.assert_called_once()
        kwargs = m.call_args.kwargs
        assert kwargs["file"]
        assert kwargs["channels"] == "channel_id"
        assert kwargs["title"] == "world_boss_tx_result"
        assert "world_boss_tx_result" in kwargs["filename"]
        for tx in fx_session.query(Transaction):
            assert tx.tx_result == "SUCCESS"


@pytest.mark.parametrize(
    "url",
    [
        "/raid/list/count",
        "/raid/rewards/list",
        "/raid/prepare",
        "/nonce",
        "/prepare-reward-assets",
        "/stage-transaction",
        "/transaction-result",
    ],
)
def test_slack_auth(fx_test_client, url: str):
    req = fx_test_client.post(url)
    assert req.status_code == 403
