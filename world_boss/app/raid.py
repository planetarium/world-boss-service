import calendar
import csv
import datetime
import hashlib
import json
import typing
from typing import List, Tuple, cast

import bencodex
import httpx
import jwt
from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy import func, insert
from sqlalchemy.orm import Session
from starlette.responses import Response

from world_boss.app.cache import cache_exists, get_from_cache, set_to_cache
from world_boss.app.config import config
from world_boss.app.enums import NetworkType
from world_boss.app.models import Transaction, WorldBossReward, WorldBossRewardAmount
from world_boss.app.schemas import WorldBossRewardSchema
from world_boss.app.stubs import (
    ActionPlainValue,
    AmountDictionary,
    CurrencyDictionary,
    RaiderWithAgentDictionary,
    RankingRewardDictionary,
    RankingRewardWithAgentDictionary,
    Recipient,
    RecipientRow,
    RewardDictionary,
    TransferAssetsValues,
)


def get_raid_rewards(
    raid_id: int, avatar_address: str, db: Session, response: Response
) -> WorldBossRewardSchema:
    avatar_address = avatar_address.replace("0x", "")

    cache_key = f"raid_rewards_{avatar_address}_{raid_id}_json"
    if cache_exists(cache_key):
        cached_value = get_from_cache(cache_key)
        cached_result = json.loads(cached_value)
        result = WorldBossRewardSchema.parse_obj(cached_result)
        response.headers["X-world-boss-service-response-cached"] = cache_key
        return result

    reward = (
        db.query(WorldBossReward)
        .filter_by(
            raid_id=raid_id,
            avatar_address=avatar_address,
        )
        .first()
    )
    if reward is None:
        raise HTTPException(status_code=404, detail="WorldBossReward not found")
    result = reward.as_schema()
    serialized = json.dumps(jsonable_encoder(result))
    set_to_cache(cache_key, serialized)
    return result


def update_agent_address(
    results: List[RankingRewardDictionary],
    raid_id: int,
    network_type: NetworkType,
    offset: int,
    limit: int,
) -> List[RankingRewardWithAgentDictionary]:
    cache_key = f"world_boss_agents_{raid_id}_{network_type}_{offset}_{limit}"
    if cache_exists(cache_key):
        cached_value = get_from_cache(cache_key)
        return json.loads(cached_value)
    else:
        http_client = httpx.Client(timeout=None, headers=get_jwt_auth_header())
        rewards: List[RankingRewardWithAgentDictionary] = []
        query_keys = [r["raider"]["address"] for r in results]
        alias_key_format = "arg{}"
        variables_format = "$arg{}: Address!"
        query_format = """
        {0}: avatar(avatarAddress: ${0}) {{
              agentAddress
              level
              name
            }}
        """
        base_query_format = """
        query({}) {{
          stateQuery {{
          {}
          }}
        }}
        """
        variables_keys = ", ".join(variables_format.format(a) for a in query_keys)
        avatar_queries = "\n".join(
            query_format.format(alias_key_format.format(i)) for i in query_keys
        )
        query = base_query_format.format(variables_keys, avatar_queries)
        variables = {}
        for avatar_address in query_keys:
            variables[f"arg{avatar_address}"] = avatar_address
        req = http_client.post(
            config.headless_url, json={"query": query, "variables": variables}
        )
        query_result = req.json()
        agents = query_result["data"]["stateQuery"]
        for result in results:
            avatar_address = result["raider"]["address"]
            r: RankingRewardWithAgentDictionary = cast(
                RankingRewardWithAgentDictionary, result
            )
            r["raider"]["agent_address"] = agents[f"arg{avatar_address}"][
                "agentAddress"
            ]
            rewards.append(r)
        set_to_cache(cache_key, json.dumps(rewards))
        return rewards


def write_ranking_rewards_csv(
    file_name: str,
    reward_list: List[RankingRewardWithAgentDictionary],
    raid_id: int,
    start_nonce: int,
    size: int,
):
    with open(file_name, "w") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "raid_id",
                "ranking",
                "agent_address",
                "avatar_address",
                "amount",
                "ticker",
                "decimal_places",
                "target_nonce",
            ]
        )
        i = 0
        for r in reward_list:
            raider: RaiderWithAgentDictionary = r["raider"]
            ranking = raider["ranking"]
            avatar_address = raider["address"]
            reward_dict_list: List[RewardDictionary] = r["rewards"]
            for reward_dict in reward_dict_list:
                currency: CurrencyDictionary = reward_dict["currency"]
                amount = reward_dict["quantity"]
                writer.writerow(
                    [
                        raid_id,
                        ranking,
                        raider["agent_address"],
                        avatar_address,
                        amount,
                        currency["ticker"],
                        currency["decimalPlaces"],
                        start_nonce + int(i / size),
                    ]
                )
                i += 1


def row_to_recipient(row: RecipientRow) -> Recipient:
    agent_address = row[2]
    avatar_address = row[3]
    amount = int(row[4])
    ticker = row[5]
    decimal_places = int(row[6])
    recipient = agent_address if ticker == "CRYSTAL" else avatar_address
    return {
        "recipient": recipient,
        "amount": {
            "quantity": amount,
            "decimalPlaces": decimal_places,
            "ticker": ticker,
        },
    }


def get_next_tx_nonce(db: Session) -> int:
    nonce = (
        db.query(func.max(Transaction.nonce))
        .filter_by(signer="0xCFCd6565287314FF70e4C4CF309dB701C43eA5bD")
        .scalar()
    )
    if nonce is None:
        return 1
    return nonce + 1


def list_tx_nonce(db: Session) -> List[int]:
    return [
        n
        for (n,) in db.query(Transaction.nonce).filter_by(
            signer="0xCFCd6565287314FF70e4C4CF309dB701C43eA5bD"
        )
    ]


def get_assets(raid_id: int, db: Session) -> List[AmountDictionary]:
    query = (
        db.query(
            func.sum(WorldBossRewardAmount.amount),
            WorldBossRewardAmount.ticker,
            WorldBossRewardAmount.decimal_places,
        )
        .join(WorldBossReward, WorldBossRewardAmount.reward)
        .filter(WorldBossReward.raid_id == raid_id)
        .group_by(WorldBossRewardAmount.ticker, WorldBossRewardAmount.decimal_places)
    )
    assets: List[AmountDictionary] = []
    for amount, ticker, decimal_places in query:
        asset: AmountDictionary = {
            "ticker": ticker,
            "quantity": int(amount),
            "decimalPlaces": decimal_places,
        }
        assets.append(asset)
    return assets


def write_tx_result_csv(file_name: str, tx_results: List[Tuple[str, str]]):
    with open(file_name, "w") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "tx_id",
                "result",
            ]
        )
        for tx_result in tx_results:
            writer.writerow([tx_result[0], tx_result[1]])


def get_currencies(db: Session) -> List[CurrencyDictionary]:
    query = db.query(
        WorldBossRewardAmount.ticker, WorldBossRewardAmount.decimal_places
    ).distinct(WorldBossRewardAmount.ticker, WorldBossRewardAmount.decimal_places)
    result: List[CurrencyDictionary] = []
    for row in query:
        currency: CurrencyDictionary = {
            "ticker": row.ticker,
            "decimalPlaces": row.decimal_places,
            "minters": [],
        }
        result.append(currency)
    return result


def get_jwt_auth_header() -> dict[str, str]:
    encoded = jwt.encode(
        {
            "exp": (datetime.datetime.now() + datetime.timedelta(minutes=5))
            .astimezone(tz=datetime.timezone.utc)
            .timestamp(),
            "iss": config.headless_jwt_iss,
        },
        config.headless_jwt_secret,
        algorithm=config.headless_jwt_algorithm,
    )
    return {"Authorization": f"Bearer {encoded}"}


def get_tx_delay_factor(index: int) -> int:
    return 4 * (index // 4)


def get_transfer_assets_plain_value(
    sender: str, recipients: typing.List[Recipient], memo: str
) -> ActionPlainValue:
    values: TransferAssetsValues = {
        "sender": bytes.fromhex(sender.replace("0x", "")),
        "recipients": [],
    }
    for r in recipients:
        amount = r["amount"]
        decimal_places = amount["decimalPlaces"]
        values["recipients"].append(
            [
                bytes.fromhex(r["recipient"].replace("0x", "")),
                [
                    {
                        "decimalPlaces": decimal_places.to_bytes(1, "big"),
                        "minters": None,
                        "ticker": amount["ticker"],
                    },
                    amount["quantity"] * 10**decimal_places,
                ],
            ]
        )
    if memo is not None:
        values["memo"] = memo
    pv: ActionPlainValue = {
        "type_id": "transfer_assets3",
        "values": values,
    }
    return pv


def create_unsigned_tx(
    planet_id: str,
    public_key: bytes,
    address: str,
    nonce: int,
    plain_value: ActionPlainValue,
    timestamp: datetime.datetime,
) -> bytes:
    """
    :param planet_id: planet id for genesis hash
    :param public_key: signer public key
    :param address: signer address
    :param nonce: tx nonce
    :param plain_value: tx actions plain value
    :param timestamp: tx time stamp
    :return: bencoded unsigned tx
    """
    if address.startswith("0x"):
        address = address[2:]
    return bencodex.dumps(
        {
            # Raw action value
            b"a": [plain_value],
            # Genesis block hash
            b"g": get_genesis_block_hash(planet_id),
            # GasLimit (see also GasLimit list section below)
            b"l": 4,
            # MaxGasPrice (see also Mead section for the currency spec)
            b"m": [
                {"decimalPlaces": b"\x12", "minters": None, "ticker": "Mead"},
                1000000000000000000,
            ],
            # Nonce
            b"n": nonce,
            # Public key
            b"p": public_key,
            # Signer
            b"s": bytes.fromhex(address),
            # Timestamp
            b"t": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            # Updated addresses
            b"u": [],
        }
    )


def append_signature_to_unsigned_tx(unsigned_tx: bytes, signature: bytes) -> bytes:
    """
    sign unsigned tx
    :param unsigned_tx:
    :param signature:
    :return: signed tx
    """
    decoded = bencodex.loads(unsigned_tx)
    decoded[b"S"] = signature
    return bencodex.dumps(decoded)


def get_genesis_block_hash(planet_id: str) -> bytes:
    switcher = {
        "0x000000000000": bytes.fromhex(
            "4582250d0da33b06779a8475d283d5dd210c683b9b999d74d03fac4f58fa6bce"
        ),
        "0x000000000001": bytes.fromhex(
            "729fa26958648a35b53e8e3905d11ec53b1b4929bf5f499884aed7df616f5913"
        ),
    }

    if planet_id not in switcher:
        raise ValueError("Invalid planet id")

    return switcher[planet_id]


def get_latest_raid_id(db: Session) -> int:
    season = db.query(func.max(WorldBossReward.raid_id)).scalar()
    if season is None:
        return 1
    return season


def get_reward_count(db: Session, raid_id: int) -> int:
    return db.query(WorldBossReward.ranking).filter_by(raid_id=raid_id).count()


def get_next_month_last_day() -> datetime.datetime:
    """
    returns the last day of the next month following the call time to ensure tx validity.
    if call this method 2024-09-xx return 2024-10-30
    """
    # Today's date
    today = datetime.date.today()

    # Calculating the year and month of the next month
    next_month_year = today.year + (today.month // 12)
    next_month = (today.month % 12) + 1

    # Getting the last day of the next month
    last_day = calendar.monthrange(next_month_year, next_month)[1]
    last_date = datetime.datetime(
        next_month_year, next_month, last_day, tzinfo=datetime.timezone.utc
    )
    return last_date


def bulk_insert_transactions(
    rows: List[RecipientRow],
    nonce_rows_map: dict[int, List[RecipientRow]],
    time_stamp: datetime.datetime,
    db: Session,
    signer,
    memo: typing.Optional[str] = None,
):
    """
    sign transfer-assets tx and save related models.
    :param rows: target recipients.
    :param nonce_rows_map: recipients group by tx nonce.
    :param time_stamp: tx time stamp.
    :param db:
    :param signer: kms signer.
    :param memo: tx memo.
    """
    # avatar_address : world_boss_reward
    world_boss_rewards: dict[str, dict] = {}
    signer_address = signer.address
    tx_values: List[dict] = []
    tx_ids: dict[int, str] = {}
    for n in nonce_rows_map.keys():
        recipient_rows = nonce_rows_map[n]
        recipients = [row_to_recipient(r) for r in recipient_rows]
        pv = get_transfer_assets_plain_value(signer_address, recipients, memo)
        unsigned_transaction = create_unsigned_tx(
            config.planet_id, signer.public_key, signer_address, n, pv, time_stamp
        )
        signed_transaction = signer.sign(unsigned_transaction, n)
        tx_id = hashlib.sha256(signed_transaction).hexdigest()
        tx_values.append(
            {
                "tx_id": tx_id,
                "nonce": n,
                "signer": signer_address,
                "payload": signed_transaction.hex(),
                "tx_result": "CREATED",
            }
        )
        tx_ids[n] = tx_id
    db.execute(insert(Transaction), tx_values)
    raid_id = int(rows[0][0])
    exist_rankings = [
        r for r, in db.query(WorldBossReward.ranking).filter_by(raid_id=raid_id)
    ]
    # avatar_address : list of world boss reward amount
    world_boss_reward_amounts: dict[str, list[dict]] = {}
    # raid_id,ranking,agent_address,avatar_address,amount,ticker,decimal_places,target_nonce
    for row in rows:
        # parse row
        ranking = int(row[1])
        agent_address = row[2]
        avatar_address = row[3]
        amount = int(row[4])
        ticker = row[5]
        decimal_places = int(row[6])
        nonce = int(row[7])

        # get or create world_boss_reward
        if ranking not in exist_rankings and not world_boss_rewards.get(avatar_address):
            world_boss_reward = {
                "raid_id": raid_id,
                "ranking": ranking,
                "agent_address": agent_address,
                "avatar_address": avatar_address,
            }
            world_boss_rewards[avatar_address] = world_boss_reward

        # create world_boss_reward_amount
        world_boss_reward_amount = {
            "amount": amount,
            "decimal_places": decimal_places,
            "ticker": ticker,
            "tx_id": tx_ids[nonce],
        }
        if not world_boss_reward_amounts.get(avatar_address):
            world_boss_reward_amounts[avatar_address] = []
        world_boss_reward_amounts[avatar_address].append(world_boss_reward_amount)
    if world_boss_rewards:
        db.execute(insert(WorldBossReward), world_boss_rewards.values())
    result = db.query(WorldBossReward).filter_by(raid_id=raid_id)
    values = []
    for reward in result:
        exist_tickers = [i.ticker for i in reward.amounts]
        if world_boss_rewards.get(reward.avatar_address):
            for amounts in world_boss_reward_amounts[reward.avatar_address]:
                if amounts["ticker"] not in exist_tickers:
                    amounts["reward_id"] = reward.id
                    values.append(amounts)
    if values:
        db.execute(insert(WorldBossRewardAmount), values)
        db.commit()
