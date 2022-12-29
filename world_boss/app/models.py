from datetime import datetime

from world_boss.app.orm import db


class WorldBossRewardAmount(db.Model):  # type: ignore
    __table_name__ = "world_boss_reward_amount"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    amount = db.Column(db.Numeric, nullable=False)
    ticker = db.Column(db.String, nullable=False)
    decimal_places = db.Column(db.Integer, nullable=False)
    tx_id = db.Column(db.String, db.ForeignKey("transaction.tx_id"), nullable=False)
    transaction = db.relationship(
        "Transaction", backref=db.backref("amounts", lazy=True)
    )
    reward_id = db.Column(
        db.Integer, db.ForeignKey("world_boss_reward.id"), nullable=False
    )
    reward = db.relationship(
        "WorldBossReward", backref=db.backref("amounts", lazy=True)
    )
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    def as_dict(self) -> dict:
        return {
            "amount": int(self.amount),
            "ticker": self.ticker,
            "tx_id": self.tx_id,
            "decimal_places": self.decimal_places,
            "tx_result": self.transaction.tx_result,
        }


class WorldBossReward(db.Model):  # type: ignore
    __table_name__ = "world_boss_reward"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    raid_id = db.Column(db.Integer, nullable=False, index=True)
    avatar_address = db.Column(db.String, nullable=False, index=True)
    agent_address = db.Column(db.String, nullable=False, index=True)
    ranking = db.Column(db.Integer, nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    # amounts = db.relationship('WorldBossRewardAmount', back_populates='reward')

    __table_args__ = (
        db.UniqueConstraint(raid_id, avatar_address, agent_address),
        db.UniqueConstraint(raid_id, ranking),
    )

    def as_dict(self) -> dict:
        return {
            "avatarAddress": self.avatar_address,
            "agentAddress": self.agent_address,
            "raidId": self.raid_id,
            "ranking": self.ranking,
            "rewards": [r.as_dict() for r in self.amounts],
        }


class Transaction(db.Model):  # type: ignore
    __table_name__ = "transaction"

    tx_id = db.Column(db.String, primary_key=True)
    tx_result = db.Column(db.String, nullable=True)
    payload = db.Column(db.String, nullable=False)
    signer = db.Column(db.String, nullable=False, index=True)
    nonce = db.Column(db.Integer, nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (db.UniqueConstraint(signer, nonce),)
