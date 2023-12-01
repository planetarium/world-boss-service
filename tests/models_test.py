from world_boss.app.models import Transaction, WorldBossReward, WorldBossRewardAmount
from world_boss.app.schemas import WorldBossRewardAmountSchema, WorldBossRewardSchema


def test_as_schema(fx_session):
    amount = 100
    ticker = "CRYSTAL"
    decimal_places = 18
    transaction = Transaction()
    transaction.tx_id = "tx_id"
    transaction.tx_result = "CREATED"
    transaction.payload = "payload"
    transaction.signer = "signer"
    transaction.nonce = 0
    reward_amount = WorldBossRewardAmount()
    reward_amount.amount = amount
    reward_amount.ticker = ticker
    reward_amount.decimal_places = decimal_places
    reward_amount.transaction = transaction

    reward = WorldBossReward()
    avatar_address = "avatar_address"
    agent_address = "agent_address"
    reward.avatar_address = avatar_address
    reward.agent_address = agent_address
    reward.raid_id = 1
    reward.ranking = 2
    reward_amount.reward = reward
    fx_session.add(reward)
    fx_session.flush()

    assert reward_amount.as_schema() == WorldBossRewardAmountSchema(
        amount=amount,
        ticker=ticker,
        tx_id="tx_id",
        decimal_places=18,
        tx_result="CREATED",
    )
    assert reward.as_schema() == WorldBossRewardSchema(
        avatarAddress=avatar_address,
        agentAddress=agent_address,
        raidId=1,
        ranking=2,
        rewards=[reward_amount.as_schema()],
    )
