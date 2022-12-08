import asyncio
import csv
import json
from typing import Union, cast, List, Tuple

import httpx
from flask import jsonify
from httpx import AsyncClient

from world_boss.app.cache import rd, set_to_cache, cache_exists
from world_boss.app.currency import Currency
from world_boss.app.data_provider import data_provider_client
from world_boss.app.enums import NetworkType
from world_boss.app.kms import MINER_URLS
from world_boss.app.models import WorldBossReward
from world_boss.app.stubs import (
    RaiderDictionary,
    RewardDictionary,
    CurrencyDictionary,
    RankingRewardDictionary,
)


def get_raid_rewards(raid_id: int, avatar_address: str):
    avatar_address = avatar_address.replace("0x", "")

    cache_key = f"raid_rewards_{avatar_address}_{raid_id}_json"
    if cache_exists(cache_key):
        cached_value = cast(Union[str, bytes], rd.get(cache_key))
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


async def to_reward_file(
    raid_id: int, file_path: str, network_type: NetworkType, start_nonce: int
):
    result: List[
        RankingRewardDictionary
    ] = await data_provider_client.get_ranking_rewards(raid_id, network_type)
    async_client = httpx.AsyncClient()
    avatar_address_list = [r["raider"]["address"] for r in result]
    agent_address_map: dict[str, str] = await get_agent_address_map(
        async_client, avatar_address_list, network_type
    )
    with open(file_path, "w") as f:
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
        for r in result:
            raider: RaiderDictionary = r["raider"]
            ranking = raider["ranking"]
            avatar_address = raider["address"]
            rewards: List[RewardDictionary] = r["rewards"]
            for reward in rewards:
                currency: CurrencyDictionary = reward["currency"]
                amount = reward["quantity"]
                writer.writerow(
                    [
                        raid_id,
                        ranking,
                        agent_address_map[avatar_address],
                        avatar_address,
                        amount,
                        currency["ticker"],
                        currency["decimalPlaces"],
                        start_nonce + int(i / 100),
                    ]
                )
                i += 1


async def get_agent_address_map(
    client: AsyncClient, avatar_address_list: List[str], network_type: NetworkType
) -> dict[str, str]:
    agent_map: dict[str, str] = {}
    result = await asyncio.gather(
        *[
            get_agent_address(client, avatar_address, network_type)
            for avatar_address in avatar_address_list
        ]
    )
    for address_map in result:
        for avatar_address in address_map:
            agent_map[avatar_address] = address_map[avatar_address]
    return agent_map


async def get_agent_address(
    client: AsyncClient, avatar_address: str, network_type: NetworkType
) -> dict[str, str]:
    query = """
    query($avatarAddress: Address!) {
      stateQuery {
        avatar(avatarAddress: $avatarAddress) {
          agentAddress
          level
          name
        }
      }
    }
    """
    variables = {"avatarAddress": avatar_address}
    result = await client.post(
        MINER_URLS[network_type], json={"query": query, "variables": variables}
    )
    return {
        avatar_address: result.json()["data"]["stateQuery"]["avatar"]["agentAddress"]
    }


async def check_total_amount(
    file_path: str, currencies: List[Tuple[str, int]], result_file_path: str
):
    currency_map: dict[str, int] = {}
    nonce_map: dict[int, dict[str, int]] = {}
    with open(file_path, "r") as f:
        reader = csv.reader(f)
        # skip header
        next(reader, None)
        # raid_id,ranking,agent_address,avatar_address,amount,ticker,decimal_places,target_nonce
        for row in reader:
            ticker = row[5]
            amount = int(row[4])
            nonce = int(row[7])
            if not nonce_map.get(nonce):
                nonce_map[nonce] = {}
            if not nonce_map[nonce].get(ticker):
                nonce_map[nonce][ticker] = 0
            nonce_map[nonce][ticker] += amount
            if not currency_map.get(ticker):
                currency_map[ticker] = 0
            currency_map[ticker] += amount
    currency_list = [i[0] for i in currencies]
    try:
        for currency in currency_map.keys():
            if currency not in currency_list:
                raise ValueError(f"missing check {currency}")
        assert len(currency_map.keys()) == len(currencies)
    except AssertionError:
        raise ValueError("missing currency. ")
    for ticker, amount in currencies:
        try:
            assert amount == currency_map[ticker]
        except AssertionError:
            raise ValueError(
                f"{ticker} total amount is wrong. given: {amount}, actual: {currency_map[ticker]}"
            )
    with open(result_file_path, "w") as f2:
        writer = csv.writer(f2)
        writer.writerow(["nonce", "ticker", "amount"])
        for nonce in nonce_map:
            for ticker in nonce_map[nonce]:
                writer.writerow(
                    [
                        nonce,
                        ticker,
                        nonce_map[nonce][ticker],
                    ]
                )
