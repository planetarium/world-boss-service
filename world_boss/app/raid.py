import csv
import datetime
import json
from typing import List, Tuple, cast

import httpx
import jwt
from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy import func
from sqlalchemy.orm import Session
from starlette.responses import Response

from world_boss.app.cache import cache_exists, get_from_cache, set_to_cache
from world_boss.app.config import config
from world_boss.app.enums import NetworkType
from world_boss.app.models import Transaction, WorldBossReward, WorldBossRewardAmount
from world_boss.app.schemas import WorldBossRewardSchema
from world_boss.app.stubs import (
    AmountDictionary,
    CurrencyDictionary,
    RaiderWithAgentDictionary,
    RankingRewardDictionary,
    RankingRewardWithAgentDictionary,
    Recipient,
    RecipientRow,
    RewardDictionary,
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
