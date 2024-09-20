from datetime import datetime

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from world_boss.app.orm import Base
from world_boss.app.schemas import WorldBossRewardAmountSchema, WorldBossRewardSchema


class WorldBossRewardAmount(Base):
    __tablename__ = "world_boss_reward_amount"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    amount: Mapped[int] = mapped_column(Numeric, nullable=False)
    ticker: Mapped[str] = mapped_column(String, nullable=False)
    decimal_places: Mapped[int] = mapped_column(Integer, nullable=False)
    tx_id: Mapped[str] = mapped_column(
        String, ForeignKey("transaction.tx_id"), nullable=False
    )
    transaction = relationship("Transaction", back_populates="amounts")
    reward_id = Column(Integer, ForeignKey("world_boss_reward.id"), nullable=False)
    reward = relationship("WorldBossReward", back_populates="amounts")
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )

    def as_dict(self) -> dict:
        return {
            "amount": int(self.amount),
            "ticker": self.ticker,
            "tx_id": self.tx_id,
            "decimal_places": self.decimal_places,
            "tx_result": self.transaction.tx_result,
        }

    def as_schema(self) -> WorldBossRewardAmountSchema:
        return WorldBossRewardAmountSchema.parse_obj(self.as_dict())


class WorldBossReward(Base):
    __tablename__ = "world_boss_reward"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    raid_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    avatar_address: Mapped[str] = mapped_column(String, nullable=False, index=True)
    agent_address: Mapped[str] = mapped_column(String, nullable=False, index=True)
    ranking: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    amounts = relationship("WorldBossRewardAmount", back_populates="reward")

    __table_args__ = (
        UniqueConstraint(raid_id, avatar_address, agent_address),
        UniqueConstraint(raid_id, ranking),
    )

    def as_dict(self) -> dict:
        return {
            "avatarAddress": self.avatar_address,
            "agentAddress": self.agent_address,
            "raidId": self.raid_id,
            "ranking": self.ranking,
            "rewards": [r.as_dict() for r in self.amounts],
        }

    def as_schema(self) -> WorldBossRewardSchema:
        return WorldBossRewardSchema.parse_obj(self.as_dict())


class Transaction(Base):
    __tablename__ = "transaction"

    tx_id: Mapped[str] = mapped_column(String, primary_key=True)
    tx_result: Mapped[str] = mapped_column(String, nullable=True)
    payload: Mapped[str] = mapped_column(String, nullable=False)
    signer: Mapped[str] = mapped_column(String, nullable=False, index=True)
    nonce: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    amounts = relationship("WorldBossRewardAmount", back_populates="transaction")

    __table_args__ = (UniqueConstraint(signer, nonce),)
