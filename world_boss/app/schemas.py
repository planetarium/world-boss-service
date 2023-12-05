import typing

from pydantic import BaseModel


class WorldBossRewardAmountSchema(BaseModel):
    amount: int
    ticker: str
    tx_id: str
    decimal_places: int
    tx_result: typing.Optional[str] = None


class WorldBossRewardSchema(BaseModel):
    avatarAddress: str
    agentAddress: str
    raidId: int
    ranking: int
    rewards: typing.List[WorldBossRewardAmountSchema]
