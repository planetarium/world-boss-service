import json
import time
import unittest.mock
from typing import List

import pytest
from pytest_httpx import HTTPXMock

from world_boss.app.config import config
from world_boss.app.enums import NetworkType
from world_boss.app.kms import signer
from world_boss.app.models import Transaction, WorldBossReward, WorldBossRewardAmount
from world_boss.app.stubs import (
    RankingRewardDictionary,
    RankingRewardWithAgentDictionary,
)
from world_boss.app.tasks import (
    check_signer_balance,
    count_users,
    get_ranking_rewards,
    insert_world_boss_rewards,
    query_tx_result,
    save_ranking_rewards,
    send_slack_message,
    sign_transfer_assets,
    stage_transaction,
    stage_transactions_with_countdown,
    upload_balance_result,
    upload_tx_result,
)


def test_count_users(celery_session_worker, httpx_mock: HTTPXMock):
    httpx_mock.add_response(
        method="POST",
        url=config.data_provider_url,
        json={"data": {"worldBossTotalUsers": 100}},
    )
    with unittest.mock.patch("world_boss.app.tasks.client.chat_postMessage") as m:
        count_users.delay("channel_id", 1).get(timeout=5)
        req = httpx_mock.get_request()
        assert req is not None
        m.assert_called_once_with(
            channel="channel_id", text="world boss season 1 total users: 100"
        )


@pytest.mark.parametrize("size", [100, 50])
def test_get_ranking_rewards(
    redisdb, celery_session_worker, httpx_mock: HTTPXMock, fx_ranking_rewards, size: int
):
    raid_id = 1
    network_type = NetworkType.MAIN
    offset = 0
    rewards_cache_key = f"world_boss_{raid_id}_{network_type}_{offset}_{size}"
    addresses_cache_key = f"world_boss_agents_{raid_id}_{network_type}_{offset}_{size}"

    # get from cache key
    cached_rewards: List[RankingRewardDictionary] = [
        {
            "raider": {
                "address": "5Ea5755eD86631a4D086CC4Fae41740C8985F1B4",
                "ranking": i + 1,
            },
            "rewards": fx_ranking_rewards,
        }
        for i in range(0, size)
    ]

    # get from service query
    requested_rewards: List[RankingRewardDictionary] = [
        {
            "raider": {
                "address": "01A0b412721b00bFb5D619378F8ab4E4a97646Ca",
                "ranking": size + 1,
            },
            "rewards": fx_ranking_rewards,
        },
    ]

    cached_addresses: List[RankingRewardWithAgentDictionary] = [
        {
            "raider": {
                "address": "5Ea5755eD86631a4D086CC4Fae41740C8985F1B4",
                "ranking": i + 1,
                "agent_address": "0xC36f031aA721f52532BA665Ba9F020e45437D98D",
            },
            "rewards": fx_ranking_rewards,
        }
        for i in range(0, size)
    ]
    redisdb.set(rewards_cache_key, json.dumps(cached_rewards))
    httpx_mock.add_response(
        method="POST",
        url=config.data_provider_url,
        json={"data": {"worldBossRankingRewards": requested_rewards}},
    )
    redisdb.set(addresses_cache_key, json.dumps(cached_addresses))
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

    with unittest.mock.patch("world_boss.app.tasks.client.files_upload_v2") as m:
        get_ranking_rewards.delay("channel_id", raid_id, size + 1, 1, size).get(
            timeout=10
        )
        m.assert_called_once()
        # skip check file. because file is temp file.
        kwargs = m.call_args.kwargs
        assert kwargs["file"]
        assert kwargs["channels"] == "channel_id"
        assert kwargs["title"] == f"world_boss_{raid_id}_{size + 1}_1_{size}_result"
        assert (
            kwargs["filename"] == f"world_boss_{raid_id}_{size + 1}_1_{size}_result.csv"
        )
    assert redisdb.exists(rewards_cache_key)
    assert redisdb.exists(addresses_cache_key)
    assert redisdb.exists(f"world_boss_{raid_id}_{network_type}_{size}_1")
    assert redisdb.exists(f"world_boss_agents_{raid_id}_{network_type}_{size}_1")


def test_get_ranking_rewards_error(
    redisdb,
    celery_session_worker,
    httpx_mock: HTTPXMock,
    fx_ranking_rewards,
):
    raid_id = 20
    network_type = NetworkType.MAIN
    offset = 0

    httpx_mock.add_response(
        method="POST",
        url=config.data_provider_url,
        json={
            "errors": [{"message": "can't receive"}],
            "data": {"worldBossRankingRewards": None},
        },
    )

    with unittest.mock.patch(
        "world_boss.app.tasks.client.chat_postMessage"
    ) as m, pytest.raises(Exception):
        get_ranking_rewards.delay("channel_id", raid_id, 101, 1, 100).get(timeout=10)
        m.assert_called_once()
        kwargs = m.call_args.kwargs
        assert (
            kwargs["text"]
            == "failed to get rewards from https://api.9c.gg/graphql exc: can't receive"
        )
        assert kwargs["channel"] == "channel_id"


@pytest.mark.parametrize(
    "nonce, max_nonce, nonce_list, expected_count",
    [
        (1, 0, [], 1),
        (1, 1, [1], 0),
        (1, 2, [1, 2], 0),
        (2, 1, [1], 1),
        (2, 3, [1, 3], 1),
    ],
)
def test_sign_transfer_assets(
    celery_session_worker,
    fx_session,
    nonce: int,
    max_nonce: int,
    nonce_list: List[int],
    expected_count: int,
):
    assert fx_session.query(Transaction).count() == 0
    sign_transfer_assets.delay(
        "2022-12-31",
        nonce,
        [],
        "memo",
        config.headless_url,
        max_nonce,
        nonce_list,
    ).get(timeout=10)
    assert fx_session.query(Transaction).count() == expected_count


@pytest.mark.parametrize("exist", [True, False])
def test_insert_world_boss_rewards(celery_session_worker, fx_session, exist: bool):
    content = """3,25,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,150000,CRYSTAL,18,175
    3,25,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,560,RUNESTONE_FENRIR1,0,175
    3,25,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,150,RUNESTONE_FENRIR2,0,175
    3,25,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,40,RUNESTONE_FENRIR3,0,175
    3,26,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,560,RUNESTONE_FENRIR1,0,175"""
    tx = Transaction()
    tx.nonce = 175
    tx.signer = "0x2531e5e06cBD11aF54f98D39578990716fFC7dBa"
    tx.tx_id = "tx_id"
    tx.payload = "payload"
    fx_session.add(tx)
    if exist:
        wb = WorldBossReward()
        wb.raid_id = 3
        wb.ranking = 25
        wb.agent_address = "0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4"
        wb.avatar_address = "5b65f5D0e23383FA18d74A62FbEa383c7D11F29d"
        wba = WorldBossRewardAmount()
        wba.reward = wb
        wba.amount = 150000
        wba.ticker = "CRYSTAL"
        wba.decimal_places = 18
        wba.tx_id = "tx_id"
        fx_session.add(wb)
    fx_session.commit()
    insert_world_boss_rewards.delay(
        [r.split(",") for r in content.split("\n")], tx.signer
    ).get(timeout=10)

    assert len(fx_session.query(Transaction).first().amounts) == 5

    world_boss_rewards = fx_session.query(WorldBossReward)
    for i, world_boss_reward in enumerate(world_boss_rewards):
        agent_address = "0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4"
        avatar_address = "5b65f5D0e23383FA18d74A62FbEa383c7D11F29d"
        ranking = 25
        amounts = [
            ("CRYSTAL", 150000, 18),
            ("RUNESTONE_FENRIR1", 560, 0),
            ("RUNESTONE_FENRIR2", 150, 0),
            ("RUNESTONE_FENRIR3", 40, 0),
        ]
        if i == 1:
            agent_address = "5b65f5D0e23383FA18d74A62FbEa383c7D11F29d"
            avatar_address = "0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4"
            ranking = 26
            amounts = [
                ("RUNESTONE_FENRIR1", 560, 0),
            ]

        assert world_boss_reward.raid_id == 3
        assert world_boss_reward.ranking == ranking
        assert world_boss_reward.agent_address == agent_address
        assert world_boss_reward.avatar_address == avatar_address

        assert len(world_boss_reward.amounts) == len(amounts)

        for ticker, amount, decimal_places in amounts:
            world_boss_reward_amount = (
                fx_session.query(WorldBossRewardAmount)
                .filter_by(reward_id=world_boss_reward.id, ticker=ticker)
                .one()
            )
            assert world_boss_reward_amount.decimal_places == decimal_places
            assert world_boss_reward_amount.amount == amount


def test_send_slack_message(
    celery_session_worker,
):
    with unittest.mock.patch("world_boss.app.tasks.client.chat_postMessage") as m:
        send_slack_message.delay("channel_id", "test message").get(timeout=3)
        m.assert_called_once_with(channel="channel_id", text="test message")


@pytest.mark.parametrize("network_type", [NetworkType.MAIN, NetworkType.INTERNAL])
def test_stage_transaction(
    celery_session_worker, fx_session, network_type: NetworkType
):
    transaction = Transaction()
    transaction.tx_id = "tx_id"
    transaction.signer = "0x2531e5e06cBD11aF54f98D39578990716fFC7dBa"
    transaction.payload = "payload"
    transaction.nonce = 1
    fx_session.add(transaction)
    fx_session.commit()
    url = config.headless_url
    with unittest.mock.patch(
        "world_boss.app.tasks.signer.stage_transaction", return_value="tx_id"
    ) as m:
        result = stage_transaction.delay(url, 1).get(timeout=3)
        m.assert_called_once()
        # for check tx class.
        call_args = m.call_args
        assert call_args[0][0] == url
        assert isinstance(call_args[0][1], Transaction)
        tx = call_args[0][1]
        assert (
            tx.tx_id == transaction.tx_id
            and tx.signer == transaction.signer
            and tx.nonce == transaction.nonce
        )
        assert result == "tx_id"


def test_query_tx_result(celery_session_worker, fx_session, fx_transactions):
    assert fx_session.query(Transaction).count() == 0
    tx_ids = []
    for transaction in fx_transactions:
        fx_session.add(transaction)
        tx_ids.append(transaction.tx_id)
    fx_session.commit()

    for tx_id in tx_ids:
        _, result = query_tx_result.delay(config.headless_url, tx_id).get(timeout=10)
        tx = fx_session.query(Transaction).filter_by(tx_id=tx_id).one()
        assert result == "INCLUDED"
        assert tx.tx_result == "INCLUDED"


def test_upload_result(
    celery_session_worker,
):
    with unittest.mock.patch("world_boss.app.tasks.client.files_upload_v2") as m:
        upload_tx_result.delay([("tx_id", "SUCCESS")], "channel_id").get(timeout=30)
        m.assert_called_once()
        # skip check file. because file is temp file.
        kwargs = m.call_args.kwargs
        assert kwargs["file"]
        assert kwargs["channels"] == "channel_id"
        assert kwargs["title"] == "world_boss_tx_result"
        assert "world_boss_tx_result" in kwargs["filename"]


@pytest.mark.parametrize(
    "ticker, decimal_places", [("CRYSTAL", 18), ("RUNESTONE_FENRIR1", 0)]
)
def test_check_signer_balance(celery_session_worker, ticker: str, decimal_places: int):
    currency = {"ticker": ticker, "decimalPlaces": decimal_places, "minters": []}
    result = check_signer_balance.delay(config.headless_url, currency).get(timeout=10)
    assert result == f"0 {ticker}"


def test_upload_balance_result(celery_session_worker):
    with unittest.mock.patch("world_boss.app.tasks.client.chat_postMessage") as m:
        upload_balance_result.delay(["0 CRYSTAL", "1 RUNE"], "channel_id").get(
            timeout=3
        )
        msg = f"world boss pool balance.\naddress:{signer.address}\n\n0 CRYSTAL\n1 RUNE"
        m.assert_called_once_with(channel="channel_id", text=msg)


def test_stage_transactions_with_countdown(
    fx_test_client,
    celery_session_worker,
    fx_session,
    fx_transactions,
):
    for tx in fx_transactions:
        fx_session.add(tx)
    fx_session.commit()
    with unittest.mock.patch(
        "world_boss.app.tasks.signer.stage_transaction", return_value="tx_id"
    ) as m, unittest.mock.patch("world_boss.app.tasks.client.chat_postMessage") as m2:
        stage_transactions_with_countdown.delay(config.headless_url, [1, 2]).get(
            timeout=30
        )
        # wait for subtask call
        time.sleep(2)
        assert m.call_count == len(fx_transactions)
        m2.assert_called_once_with(
            channel=config.slack_channel_id,
            text=f"stage {len(fx_transactions)} transactions",
        )


def test_save_ranking_rewards(
    redisdb,
    celery_session_worker,
    httpx_mock: HTTPXMock,
    fx_ranking_reward_csv,
    fx_session,
):
    raid_id = 1
    network_type = NetworkType.MAIN
    rewards: dict[str, RankingRewardDictionary] = {}
    graphql_result: dict = {}
    # raid_id,ranking,agent_address,avatar_address,amount,ticker,decimal_places,target_nonce
    for line in fx_ranking_reward_csv.split("\n"):
        row = line.split(",")
        avatar_address = row[3]
        if not rewards.get(avatar_address):
            rewards[avatar_address] = {
                "raider": {
                    "address": avatar_address,
                    "ranking": int(row[1]),
                },
                "rewards": [],
            }
            graphql_result[f"arg{avatar_address}"] = {"agentAddress": row[2]}
        rewards[avatar_address]["rewards"].append(
            {
                "currency": {
                    "decimalPlaces": row[6],
                    "minters": None,
                    "ticker": row[5],
                },
                "quantity": row[4],
            }
        )

    # get from service query
    requested_rewards: List[RankingRewardDictionary] = list(rewards.values())
    httpx_mock.add_response(
        method="POST",
        url=config.data_provider_url,
        json={"data": {"worldBossRankingRewards": requested_rewards}},
    )
    httpx_mock.add_response(
        method="POST",
        url=config.headless_url,
        json={"data": {"stateQuery": graphql_result}},
    )
    save_ranking_rewards(raid_id, 500, 50, len(rewards))
    assert redisdb.exists(f"world_boss_{raid_id}_{network_type}_0_500")
    assert redisdb.exists(f"world_boss_agents_{raid_id}_{network_type}_0_500")
    query = fx_session.query(Transaction)
    assert query.count() == 10
    for tx in query:
        assert len(tx.amounts) <= 50
    assert fx_session.query(WorldBossReward).filter_by(raid_id=raid_id).count() == 125
    assert fx_session.query(WorldBossRewardAmount).count() == 500
