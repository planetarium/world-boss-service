import argparse
import asyncio
import csv
from datetime import datetime
from multiprocessing import Pool
from typing import List

from world_boss.app.enums import NetworkType
from world_boss.app.kms import signer, MINER_URLS
from world_boss.app.models import Transaction


async def main(
    file_path: str,
    time_stamp: datetime,
    network_type: NetworkType,
    result_file_path: str,
):
    recipients_map: dict[int, list[dict]] = {}
    print("start prepare recipients")
    start = datetime.utcnow()
    with open(file_path) as f:
        reader = csv.reader(f)
        # skip header
        next(reader, None)
        # raid_id, ranking, agent_address, avatar_address, amount, ticker, decimal_places, target_nonce
        for row in reader:
            nonce = int(row[7])
            recipient = to_recipient(row)
            if not recipients_map.get(nonce):
                recipients_map[nonce] = []
            recipients_map[nonce].append(recipient)
    end = datetime.utcnow()
    print(f"complete prepare recipients. {end - start}")
    with open(result_file_path, "w") as f:
        writer = csv.writer(f)
        writer.writerow(["nonce", "tx_id", "signer", "payload"])
    memo = f"world boss season {row[0]} ranking rewards"
    print("start sign transfer assets")
    start = datetime.utcnow()
    transactions: List[Transaction] = []
    transactions = await asyncio.gather(
        *[
            signer.transfer_assets(
                time_stamp, nonce, recipients_map[nonce], memo, MINER_URLS[network_type]
            )
            for nonce in recipients_map
        ]
    )
    end = datetime.utcnow()
    print(f"complete sign transfer assets. {end - start}")
    print("start write tx file")
    start = datetime.utcnow()
    for tx in transactions:
        with open(result_file_path, "a") as f2:
            writer = csv.writer(f2)
            writer.writerow([tx.nonce, tx.tx_id, tx.signer, tx.payload])
    end = datetime.utcnow()
    print(f"complete write tx file. {end - start}")


def to_recipient(row):
    agent_address = row[2]
    avatar_address = row[3]
    amount = int(row[4])
    ticker = row[5]
    decimal_places = int(row[6])
    recipient = agent_address if ticker == "CRYSTAL" else avatar_address
    return {
        "recipient": recipient,
        "amount": {
            "quantity": amount,
            "decimalPlaces": decimal_places,
            "ticker": ticker,
        },
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=str)
    parser.add_argument("--time-stamp", type=datetime.fromisoformat)
    parser.add_argument("--network", type=str, choices=["MAIN", "INTERNAL"])
    parser.add_argument("--tx-file-path", type=str)
    args = parser.parse_args()
    network_type = NetworkType.MAIN if args.network == "MAIN" else NetworkType.INTERNAL
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        main(args.path, args.time_stamp, network_type, args.tx_file_path)
    )
    loop.close()
