import csv
import math

from sqlalchemy import bindparam
from sqlalchemy.dialects.postgresql import insert

from world_boss.app.models import Transaction, WorldBossReward, WorldBossRewardAmount
from world_boss.app.orm import db
from world_boss.wsgi import app


def migrate_tx():
    with app.app_context():
        values = []
        for raid_id in [1, 2]:
            tx_file_path = f"world_boss_season_{raid_id}_txs.csv"
            with open(tx_file_path, 'r') as f2:
                reader = csv.reader(f2)
                # skip header
                next(reader, None)
                # nonce,tx_id,hexed_tx
                for row in reader:
                    nonce = int(row[0])
                    tx_id = row[1]
                    values.append({'nonce': nonce, 'tx_id': tx_id, 'payload': row[2]})

        # execute tx insert
        signer = '0xCFCd6565287314FF70e4C4CF309dB701C43eA5bD'
        tx_table = Transaction.__table__
        insert_stmt = insert(tx_table).values({
            'tx_id': bindparam('tx_id'),
            'nonce': bindparam('nonce'),
            'signer': signer,
            'payload': bindparam('payload'),
            'tx_result': 'SUCCESS',
        })
        do_nothing_stmt = insert_stmt.on_conflict_do_nothing()
        db.session.execute(do_nothing_stmt, values)
        db.session.commit()


def get_nonce(raid_id: int, ranking: int, ticker: str) -> int:
    if ticker == "RUNESTONE_FENRIR1":
        nonce_interval = 0
    elif ticker == "RUNESTONE_FENRIR2":
        nonce_interval = 20 if raid_id == 1 else 19
    else:
        nonce_interval = 40 if raid_id == 1 else 38
    offset = 0 if raid_id == 1 else 54
    nonce = int(math.ceil(ranking / 100)) + nonce_interval + offset
    return nonce


def migrate_ranking():
    with app.app_context():
        amount_map = {}
        tx_map = {}
        for raid_id in [1, 2]:
            file_path = f"20221124.csv" if raid_id == 1 else f"20221124_s2.csv"
            tx_file_path = f"world_boss_season_{raid_id}_txs.csv"
            with open(tx_file_path, 'r') as f2:
                reader = csv.reader(f2)
                # skip header
                next(reader, None)
                # nonce,tx_id,hexed_tx
                for row in reader:
                    nonce = int(row[0])
                    tx_id = row[1]
                    tx_map[nonce] = tx_id

            with open(file_path, 'r') as f:
                reader = csv.reader(f)
                # skip header
                next(reader, None)
                # ranking,avatar_address,ticker,amount
                for row in reader:
                    ranking = int(row[0])
                    ticker = row[2]
                    nonce = get_nonce(raid_id, ranking, ticker)
                    value = {
                        'ticker': ticker,
                        'amount': int(row[3]),
                        'tx_id': tx_map[nonce],
                    }
                    if not amount_map.get(raid_id):
                        amount_map[raid_id] = {}
                    if not amount_map[raid_id].get(ranking):
                        amount_map[raid_id][ranking] = []
                    amount_map[raid_id][ranking].append(value)
        with open('ranking.csv', 'r') as f:
            values = []
            models = []
            reader = csv.reader(f)
            # skip head
            next(reader, None)
            # raid_id,ranking,agent_address,avatar_address
            for row in reader:
                reward = WorldBossReward()
                raid_id = int(row[0])
                avatar_address = row[3]
                agent_address = row[2]
                ranking = int(row[1])
                reward.ranking = ranking
                reward.raid_id = raid_id
                reward.avatar_address = avatar_address
                reward.agent_address = agent_address
                for amount in amount_map[raid_id][ranking]:
                    model = WorldBossRewardAmount()
                    model.ticker = amount['ticker']
                    model.amount = amount['amount']
                    model.tx_id = amount['tx_id']
                    model.reward = reward
                    model.decimal_places = 0
                models.append(model)
            db.session.add_all(models)
            db.session.commit()


def migrate_crystal():
    with app.app_context():
        tx_nonce_map = {}
        values = []
        with open('tx_nonce.csv') as f:
            reader = csv.reader(f)
            # skip head
            next(reader, None)
            # nonce,tx_id,b_id
            for row in reader:
                nonce = int(row[0])
                tx_id = row[1]
                tx_nonce_map[tx_id] = nonce
                values.append({
                    'tx_id': row[1],
                    'nonce': nonce,
                    'payload': 'migrated world boss ranking crystal tx',
                })
        with open('tx_failed.csv') as f2:
            reader = csv.reader(f2)
            # b_id,tx_id
            for row in reader:
                tmp_nonce = int(row[0])
                failed_tx_id = row[1]
                tx_nonce_map[failed_tx_id] = tmp_nonce
                values.append({
                    'tx_id': failed_tx_id,
                    'nonce': tmp_nonce,
                    'payload': 'failed world boss ranking crystal tx. retry required.',
                })

        # execute tx insert
        crystal_signer = '0x491d9842ed8f1b5d291272cf9e7b66a7b7c90cda'
        tx_table = Transaction.__table__
        insert_stmt = insert(tx_table).values({
            'tx_id': bindparam('tx_id'),
            'nonce': bindparam('nonce'),
            'signer': crystal_signer,
            'payload': bindparam('payload'),
            'tx_result': 'SUCCESS',
        })
        do_nothing_stmt = insert_stmt.on_conflict_do_nothing()
        db.session.execute(do_nothing_stmt, values)
        db.session.commit()

        reward_map = {}
        rewards = db.session.query(WorldBossReward).all()
        amount_values = []
        for reward in rewards:
            raid_id = reward.raid_id
            ranking = reward.ranking
            reward_id = reward.id
            if not reward_map.get(reward.raid_id):
                reward_map[raid_id] = {}
            reward_map[raid_id][ranking] = reward_id
        with open('crystal.csv') as f3:
            reader = csv.reader(f3)
            # skip head
            next(reader, None)
            # raid_id | ranking | amount | tx_id
            for row in reader:
                raid_id = int(row[0])
                ranking = int(row[1])
                amount = int(row[2])
                amount_values.append({
                    'tx_id': row[3],
                    'amount': amount,
                    'b_id': reward_map[raid_id][ranking]
                })

        amount_table = WorldBossRewardAmount.__table__
        insert_stmt = insert(amount_table).values({
            'tx_id': bindparam('tx_id'),
            'ticker': 'CRYSTAL',
            'reward_id': bindparam('b_id'),
            'decimal_places': 18,
            'amount': bindparam('amount'),
        })
        do_nothing_stmt = insert_stmt.on_conflict_do_nothing()
        db.session.execute(do_nothing_stmt, amount_values)
        db.session.commit()


if __name__ == "__main__":
    migrate_tx()
    migrate_ranking()
    migrate_crystal()
