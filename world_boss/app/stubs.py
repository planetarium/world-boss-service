from typing import Annotated, List, Optional

from mypy_extensions import TypedDict
from typing_extensions import NotRequired

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
RecipientRow = Annotated[List[str], 8]
AmountDictionary = TypedDict(
    "AmountDictionary",
    {
        "ticker": str,
        "decimalPlaces": int,
        "quantity": int,
    },
)
Recipient = TypedDict("Recipient", {"recipient": str, "amount": AmountDictionary})
TransferAssetsValues = TypedDict(
    "TransferAssetsValues",
    {
        "sender": bytes,
        "recipients": list,
        "memo": NotRequired[str],
    },
)
ActionPlainValue = TypedDict(
    "ActionPlainValue",
    {
        "type_id": str,
        "values": TransferAssetsValues,
    },
)
