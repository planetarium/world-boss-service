import typing
from datetime import date, datetime, timezone
from typing import List
from unittest.mock import patch

import bencodex
import pytest
from sqlalchemy.orm import Session

from world_boss.app.cache import cache_exists
from world_boss.app.enums import NetworkType
from world_boss.app.kms import signer
from world_boss.app.models import Transaction, WorldBossReward, WorldBossRewardAmount
from world_boss.app.raid import (
    bulk_insert_transactions,
    create_unsigned_tx,
    get_assets,
    get_claim_items_plain_value,
    get_latest_raid_id,
    get_next_month_last_day,
    get_next_tx_nonce,
    get_reward_count,
    get_transfer_assets_plain_value,
    get_tx_delay_factor,
    list_tx_nonce,
    update_agent_address,
    write_ranking_rewards_csv,
    write_tx_result_csv,
)
from world_boss.app.stubs import (
    ActionPlainValue,
    AmountDictionary,
    ClaimItemsValues,
    RankingRewardDictionary,
    RankingRewardWithAgentDictionary,
    Recipient,
    TransferAssetsValues,
)


@pytest.mark.parametrize("network_type", [NetworkType.MAIN, NetworkType.INTERNAL])
@pytest.mark.parametrize("caching", [True, False])
@pytest.mark.parametrize("offset", [0, 1])
@pytest.mark.parametrize("limit", [1, 2])
def test_update_agent_address(
    redis_proc, network_type: NetworkType, caching: bool, offset: int, limit: int
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
@pytest.mark.parametrize(
    "start_nonce, bottom, size, last_nonce",
    [(1, 100, 100, 6), (1, 100, 50, 12), (2, 4, 100, 2), (2, 4, 1, 25)],
)
def test_write_ranking_rewards_csv(
    tmp_path,
    fx_ranking_rewards,
    raid_id: int,
    start_nonce: int,
    bottom: int,
    size: int,
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
    write_ranking_rewards_csv(file_name, reward_list, raid_id, start_nonce, size)
    with open(file_name, "r") as f:
        rows = f.readlines()
        # header + fx_ranking_rewards * bottom
        assert len(rows) == 1 + (bottom * 6)
        # check header
        assert (
            rows[0]
            == "raid_id,ranking,agent_address,avatar_address,amount,ticker,decimal_places,target_nonce\n"
        )

        # check first and last row
        for key, ranking, amount, ticker, decimal_places, nonce in [
            (1, 1, 1000000, "CRYSTAL", 18, start_nonce),
            (-1, bottom, 300, "Item_NT_800201", 0, last_nonce),
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
    assert get_next_tx_nonce(fx_session) == expected


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
    assert get_next_tx_nonce(fx_session) == 1


def test_get_assets(fx_session) -> None:
    assets: List[AmountDictionary] = [
        {"decimalPlaces": 18, "ticker": "CRYSTAL", "quantity": 109380000},
        {"decimalPlaces": 0, "ticker": "RUNESTONE_FENRIR1", "quantity": 406545},
        {"decimalPlaces": 0, "ticker": "RUNESTONE_FENRIR2", "quantity": 111715},
        {"decimalPlaces": 0, "ticker": "RUNESTONE_FENRIR3", "quantity": 23890},
    ]
    for i in range(1, 5):
        asset = assets[i - 1]
        transaction = Transaction()
        transaction.tx_id = str(i)
        transaction.signer = "signer"
        transaction.payload = "payload"
        transaction.nonce = i
        reward = WorldBossReward()
        reward.avatar_address = "avatar_address"
        reward.agent_address = "agent_address"
        reward.raid_id = i
        reward.ranking = i
        reward_amount = WorldBossRewardAmount()
        reward_amount.amount = asset["quantity"]
        reward_amount.ticker = asset["ticker"]
        reward_amount.decimal_places = asset["decimalPlaces"]
        reward_amount.reward = reward
        reward_amount.transaction = transaction
        fx_session.add(transaction)
    fx_session.commit()
    for i, asset in enumerate(assets):
        raid_id = i + 1
        assert get_assets(raid_id, fx_session) == [assets[i]]


def test_write_tx_result_csv(tmp_path):
    file_name = tmp_path / "test.csv"
    results = [
        ("1", "SUCCESS"),
        ("2", "FAILURE"),
    ]
    write_tx_result_csv(file_name, results)
    with open(file_name, "r") as f:
        rows = f.readlines()
        # check header
        assert rows[0] == "tx_id,result\n"

        # check first and last row
        for key, (tx_id, result) in enumerate(results):
            assert rows[key + 1] == f"{tx_id},{result}\n"


@pytest.mark.parametrize(
    "nonce_list",
    [
        ([]),
        ([1, 2]),
        ([2, 3]),
        ([1, 4]),
        ([5]),
    ],
)
def test_list_tx_nonce(fx_session, nonce_list: List[int]):
    for nonce in nonce_list:
        tx = Transaction()
        tx.nonce = nonce
        tx.tx_id = str(nonce)
        tx.signer = "0xCFCd6565287314FF70e4C4CF309dB701C43eA5bD"
        tx.payload = "payload"
        fx_session.add(tx)
    fx_session.flush()
    assert list_tx_nonce(fx_session) == nonce_list


@pytest.mark.parametrize(
    "expected, index",
    [
        (0, 0),
        (0, 1),
        (0, 2),
        (0, 3),
        (4, 4),
        (4, 5),
        (8, 8),
    ],
)
def test_get_tx_delay_factor(expected: int, index: int):
    assert get_tx_delay_factor(index) == expected


@pytest.mark.parametrize("memo", ["memo", None])
def test_get_transfer_assets_plain_value(memo: str):
    sender = "0xCFCd6565287314FF70e4C4CF309dB701C43eA5bD"
    recipients: List[Recipient] = [
        {
            "recipient": "0x2531e5e06cBD11aF54f98D39578990716fFC7dBa",
            "amount": {
                "quantity": 10,
                "decimalPlaces": 18,
                "ticker": "CRYSTAL",
            },
        },
        {
            "recipient": "0x2531e5e06cBD11aF54f98D39578990716fFC7dBa",
            "amount": {
                "quantity": 100,
                "decimalPlaces": 0,
                "ticker": "RUNESTONE_FENRIR1",
            },
        },
    ]
    plain_value: ActionPlainValue = get_transfer_assets_plain_value(
        sender, recipients, memo
    )
    assert plain_value["type_id"] == "transfer_assets3"
    values: TransferAssetsValues = plain_value["values"]  # type: ignore
    assert values["sender"] == bytes.fromhex(sender.replace("0x", ""))
    assert values["recipients"] == [
        [
            bytes.fromhex("2531e5e06cBD11aF54f98D39578990716fFC7dBa"),
            [
                {
                    "decimalPlaces": b"\x12",
                    "minters": None,
                    "ticker": "CRYSTAL",
                },
                10000000000000000000,
            ],
        ],
        [
            bytes.fromhex("2531e5e06cBD11aF54f98D39578990716fFC7dBa"),
            [
                {
                    "decimalPlaces": b"\x00",
                    "minters": None,
                    "ticker": "RUNESTONE_FENRIR1",
                },
                100,
            ],
        ],
    ]
    assert values.get("memo") == memo


@pytest.mark.parametrize(
    "planet_id, genesis",
    [
        (
            "0x000000000000",
            "4582250d0da33b06779a8475d283d5dd210c683b9b999d74d03fac4f58fa6bce",
        ),
        (
            "0x000000000001",
            "729fa26958648a35b53e8e3905d11ec53b1b4929bf5f499884aed7df616f5913",
        ),
    ],
)
def test_create_unsigned_tx(
    planet_id: str, genesis: str, fx_transfer_assets_plain_value
):
    public_key = signer.public_key
    address = signer.address
    actual = create_unsigned_tx(
        planet_id,
        public_key,
        address,
        1,
        fx_transfer_assets_plain_value,
        datetime(2024, 9, 30, tzinfo=timezone.utc),
    )
    expected = {
        b"a": [fx_transfer_assets_plain_value],
        b"g": bytes.fromhex(genesis),
        b"l": 4,
        b"m": [
            {"decimalPlaces": b"\x12", "minters": None, "ticker": "Mead"},
            1000000000000000000,
        ],
        b"n": 1,
        b"p": public_key,
        b"s": bytes.fromhex(address.replace("0x", "")),
        b"t": "2024-09-30T00:00:00.000000Z",
        b"u": [],
    }
    assert bencodex.dumps(expected) == actual


@pytest.mark.parametrize("season, expected", [(None, 1), (1, 1), (2, 2)])
def test_get_latest_season(
    fx_session: Session, season: typing.Optional[int], expected: int
):
    if season:
        reward = WorldBossReward()
        reward.raid_id = season
        avatar_address = "avatar_address"
        agent_address = "agent_address"
        reward.avatar_address = avatar_address
        reward.agent_address = agent_address
        reward.ranking = 1
        fx_session.add(reward)
        fx_session.commit()
    assert get_latest_raid_id(fx_session) == expected


def test_get_reward_count(fx_session: Session):
    reward = WorldBossReward()
    reward.raid_id = 1
    avatar_address = "avatar_address"
    agent_address = "agent_address"
    reward.avatar_address = avatar_address
    reward.agent_address = agent_address
    reward.ranking = 1
    fx_session.add(reward)
    fx_session.commit()
    assert get_reward_count(fx_session, 1) == 1
    assert get_reward_count(fx_session, 2) == 0


def test_get_next_month_last_day():
    with patch("datetime.date") as m:
        m.today.return_value = date(2024, 9, 19)
        assert get_next_month_last_day() == datetime(2024, 10, 31, tzinfo=timezone.utc)


def test_bulk_insert_transactions(fx_session):
    content = """3,25,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,150000,CRYSTAL,18,175
    3,25,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,560,RUNESTONE_FENRIR1,0,175
    3,25,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,150,RUNESTONE_FENRIR2,0,175
    3,25,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,40,RUNESTONE_FENRIR3,0,175
    3,25,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,560,RUNESTONE_FENRIR1,0,175
    3,25,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,300,Item_NT_500000,0,175
    3,25,5b65f5D0e23383FA18d74A62FbEa383c7D11F29d,0x01069aaf336e6aEE605a8A54D0734b43B62f8Fe4,30,Item_NT_800201,0,175"""
    rows = [r.split(",") for r in content.split("\n")]
    nonce_rows_map = {175: rows}
    bulk_insert_transactions(
        rows,
        nonce_rows_map,
        datetime(2024, 9, 24, tzinfo=timezone.utc),
        fx_session,
        signer,
        "memo",
    )

    assert len(fx_session.query(Transaction).first().amounts) == 7

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
            amounts = [
                ("RUNESTONE_FENRIR1", 560, 0),
                ("Item_NT_500000", 300, 0),
                ("Item_NT_800201", 30, 0),
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


@pytest.mark.parametrize("memo", ["memo", None])
def test_get_claim_items_plain_value(memo: str):
    recipients: List[Recipient] = [
        {
            "recipient": "0x2531e5e06cBD11aF54f98D39578990716fFC7dBa",
            "amount": {
                "quantity": 10,
                "decimalPlaces": 18,
                "ticker": "CRYSTAL",
            },
        },
        {
            "recipient": "0x2531e5e06cBD11aF54f98D39578990716fFC7dBa",
            "amount": {
                "quantity": 100,
                "decimalPlaces": 0,
                "ticker": "RUNESTONE_FENRIR1",
            },
        },
        {
            "recipient": "0x2531e5e06cBD11aF54f98D39578990716fFC7dBa",
            "amount": {
                "quantity": 100,
                "decimalPlaces": 0,
                "ticker": "Item_NT_500000",
            },
        },
    ]
    plain_value: ActionPlainValue = get_claim_items_plain_value(recipients, memo)
    assert plain_value["type_id"] == "claim_items"
    values: ClaimItemsValues = plain_value["values"]  # type: ignore
    assert values["cd"] == [
        [
            bytes.fromhex("2531e5e06cBD11aF54f98D39578990716fFC7dBa"),
            [
                [
                    {
                        "decimalPlaces": b"\x12",
                        "minters": None,
                        "ticker": "FAV__CRYSTAL",
                    },
                    10000000000000000000,
                ],
                [
                    {
                        "decimalPlaces": b"\x00",
                        "minters": None,
                        "ticker": "FAV__RUNESTONE_FENRIR1",
                    },
                    100,
                ],
                [
                    {
                        "decimalPlaces": b"\x00",
                        "minters": None,
                        "ticker": "Item_NT_500000",
                    },
                    100,
                ],
            ],
        ],
    ]
    assert values.get("m") == memo
