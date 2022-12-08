from typing import Union, Optional, List, Dict

from mypy_extensions import TypedDict

RaiderDictionary = TypedDict("RaiderDictionary", {"address": str, "ranking": int})
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
