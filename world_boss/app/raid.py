import csv
import json
from typing import List, Tuple, cast

import httpx
from flask import jsonify
from sqlalchemy import func

from world_boss.app.cache import cache_exists, get_from_cache, set_to_cache
from world_boss.app.enums import NetworkType
from world_boss.app.kms import MINER_URLS
from world_boss.app.models import Transaction, WorldBossReward, WorldBossRewardAmount
from world_boss.app.orm import db
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


def get_raid_rewards(raid_id: int, avatar_address: str):
    avatar_address = avatar_address.replace("0x", "")

    cache_key = f"raid_rewards_{avatar_address}_{raid_id}_json"
    if cache_exists(cache_key):
        cached_value = get_from_cache(cache_key)
        cached_result = json.loads(cached_value)
        resp = jsonify(cached_result)
        resp.headers["X-world-boss-service-response-cached"] = cache_key
        return resp

    reward = WorldBossReward.query.filter_by(
        raid_id=raid_id,
        avatar_address=avatar_address,
    ).first_or_404()
    result = reward.as_dict()
    set_to_cache(cache_key, json.dumps(result))
    return jsonify(result)


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
        http_client = httpx.Client(timeout=30)
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
            MINER_URLS[NetworkType.MAIN], json={"query": query, "variables": variables}
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
                        start_nonce + int(i / 100),
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


def get_next_tx_nonce() -> int:
    nonce = (
        db.session.query(func.max(Transaction.nonce))
        .filter_by(signer="0xCFCd6565287314FF70e4C4CF309dB701C43eA5bD")
        .scalar()
    )
    if nonce is None:
        return 1
    return nonce + 1


def get_assets(raid_id: int) -> List[AmountDictionary]:
    query = (
        db.session.query(
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


def get_currencies() -> List[CurrencyDictionary]:
    query = db.session.query(
        WorldBossRewardAmount.ticker, WorldBossRewardAmount.decimal_places
    ).distinct(WorldBossRewardAmount.ticker, WorldBossRewardAmount.decimal_places)
    result: List[CurrencyDictionary] = []
    for row in query:
        currency: CurrencyDictionary = {
            "ticker": row.ticker,
            "decimalPlaces": row.decimal_places,
            "minters": None,
        }
        result.append(currency)
    return result
