from typing import Union, Optional, List, Dict

from mypy_extensions import TypedDict

RaiderDictionary = TypedDict("RaiderDictionary", {"address": str, "ranking": int})
RaiderWithAgentDictionary = TypedDict(
    "RaiderWithAgentDictionary", {"address": str, "ranking": int, "agent_address": str}
)
CurrencyDictionary = TypedDict(
    "CurrencyDictionary",
    {"ticker": str, "decimalPlaces": int, "minters": Optional[List[str]]},
)
RewardDictionary = TypedDict(
    "RewardDictionary", {"quantity": str, "currency": CurrencyDictionary}
)
RankingRewardDictionary = TypedDict(
    "RankingRewardDictionary",
    {"raider": RaiderDictionary, "rewards": List[RewardDictionary]},
)
RankingRewardWithAgentDictionary = TypedDict(
    "RankingRewardWithAgentDictionary",
    {"raider": RaiderWithAgentDictionary, "rewards": List[RewardDictionary]},
)
