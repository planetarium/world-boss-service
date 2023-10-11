import unittest
from unittest.mock import MagicMock

import pytest
from celery.result import AsyncResult
from pytest_httpx import HTTPXMock

from world_boss.app.models import Transaction, WorldBossReward, WorldBossRewardAmount


@pytest.fixture()
def non_mocked_hosts() -> list:
    return ["9c-main-full-state.nine-chronicles.com"]


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
