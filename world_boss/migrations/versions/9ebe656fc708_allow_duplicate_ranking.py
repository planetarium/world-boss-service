"""allow duplicate ranking

Revision ID: 9ebe656fc708
Revises: 4321181b2dfb
Create Date: 2024-09-26 11:28:43.980961

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "9ebe656fc708"
down_revision = "4321181b2dfb"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint(
        "world_boss_reward_raid_id_avatar_address_agent_address_key",
        "world_boss_reward",
    )
    op.drop_constraint("world_boss_reward_raid_id_ranking_key", "world_boss_reward")
    op.create_unique_constraint(
        "world_boss_reward_raid_id_address_ranking_key",
        "world_boss_reward",
        ["raid_id", "avatar_address", "agent_address", "ranking"],
    )


def downgrade():
    op.drop_constraint(
        "world_boss_reward_raid_id_address_ranking_key",
        "world_boss_reward",
    )
    op.create_unique_constraint(
        "world_boss_reward_raid_id_avatar_address_agent_address_key",
        "world_boss_reward",
        ["raid_id", "avatar_address", "agent_address"],
    )
    op.create_unique_constraint(
        "world_boss_reward_raid_id_ranking_key",
        "world_boss_reward",
        ["raid_id", "ranking"],
    )
