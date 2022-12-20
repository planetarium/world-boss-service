from typing import List

import pytest

from world_boss.app.cache import cache_exists
from world_boss.app.enums import NetworkType
from world_boss.app.models import Transaction
from world_boss.app.raid import (
    get_next_tx_nonce,
    update_agent_address,
    write_ranking_rewards_csv,
)
from world_boss.app.stubs import (
    RankingRewardDictionary,
    RankingRewardWithAgentDictionary,
)


@pytest.mark.parametrize("network_type", [NetworkType.MAIN, NetworkType.INTERNAL])
@pytest.mark.parametrize("caching", [True, False])
@pytest.mark.parametrize("offset", [0, 1])
@pytest.mark.parametrize("limit", [1, 2])
def test_update_agent_address(
    redisdb, network_type: NetworkType, caching: bool, offset: int, limit: int
):
    cache_key = f"world_boss_agents_1_{network_type}_{offset}_{limit}"
    rewards: List[RankingRewardDictionary] = [
        {
            "raider": {
                "address": "5Ea5755eD86631a4D086CC4Fae41740C8985F1B4",
                "ranking": 1,
            },
            "rewards": [],
        },
        {
            "raider": {
                "address": "01A0b412721b00bFb5D619378F8ab4E4a97646Ca",
                "ranking": 2,
            },
            "rewards": [],
        },
    ]
    expected_result: List[RankingRewardWithAgentDictionary] = [
        {
            "raider": {
                "address": "5Ea5755eD86631a4D086CC4Fae41740C8985F1B4",
                "ranking": 1,
                "agent_address": "0xC36f031aA721f52532BA665Ba9F020e45437D98D",
            },
            "rewards": [],
        },
        {
            "raider": {
                "address": "01A0b412721b00bFb5D619378F8ab4E4a97646Ca",
                "ranking": 2,
                "agent_address": "0x9EBD1b4F9DbB851BccEa0CFF32926d81eDf6De52",
            },
            "rewards": [],
        },
    ]
    result: List[RankingRewardWithAgentDictionary] = update_agent_address(
        rewards, 1, network_type, offset, limit
    )
    assert result == expected_result
    assert cache_exists(cache_key)


@pytest.mark.parametrize("raid_id", [1, 2])
@pytest.mark.parametrize("start_nonce, bottom, last_nonce", [(1, 100, 4), (2, 4, 2)])
def test_write_ranking_rewards_csv(
    tmp_path,
    fx_ranking_rewards,
    raid_id: int,
    start_nonce: int,
    bottom: int,
    last_nonce: int,
):
    file_name = tmp_path / "test.csv"
    avatar_address = "5Ea5755eD86631a4D086CC4Fae41740C8985F1B4"
    agent_address = "0xC36f031aA721f52532BA665Ba9F020e45437D98D"
    reward_list: List[RankingRewardWithAgentDictionary] = [
        {
            "raider": {
                "address": avatar_address,
                "ranking": i + 1,
                "agent_address": agent_address,
            },
            "rewards": fx_ranking_rewards,
        }
        for i in range(0, bottom)
    ]
    write_ranking_rewards_csv(file_name, reward_list, raid_id, start_nonce)
    with open(file_name, "r") as f:
        rows = f.readlines()
        # check header
        assert (
            rows[0]
            == "raid_id,ranking,agent_address,avatar_address,amount,ticker,decimal_places,target_nonce\n"
        )

        # check first and last row
        for key, ranking, amount, ticker, decimal_places, nonce in [
            (1, 1, 1000000, "CRYSTAL", 18, start_nonce),
            (-1, bottom, 300, "RUNESTONE_FENRIR3", 0, last_nonce),
        ]:
            assert (
                rows[key]
                == f"{raid_id},{ranking},{agent_address},{avatar_address},{amount},{ticker},{decimal_places},{nonce}\n"
            )


@pytest.mark.parametrize(
    "nonce_list, expected",
    [
        ([1, 2], 3),
        ([2, 3], 4),
        ([1, 4], 5),
        ([5], 6),
    ],
)
def test_get_next_tx_nonce(fx_session, nonce_list: List[int], expected: int):
    for nonce in nonce_list:
        tx = Transaction()
        tx.nonce = nonce
        tx.tx_id = str(nonce)
        tx.signer = "0xCFCd6565287314FF70e4C4CF309dB701C43eA5bD"
        tx.payload = "payload"
        fx_session.add(tx)
    fx_session.flush()
    assert get_next_tx_nonce() == expected


@pytest.mark.parametrize("tx_exist", [True, False])
def test_get_next_tx_nonce_tx_empty(fx_session, tx_exist: bool):
    if tx_exist:
        tx = Transaction()
        tx.nonce = 1
        tx.tx_id = "tx_id"
        tx.signer = "signer"
        tx.payload = "payload"
        fx_session.add(tx)
        fx_session.flush()
    assert get_next_tx_nonce() == 1
