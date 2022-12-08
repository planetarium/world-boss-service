import argparse
import asyncio
import sys

from world_boss.app.enums import NetworkType
from world_boss.app.raid import to_reward_file


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int)
    parser.add_argument("--path", type=str)
    parser.add_argument("--network", type=str, choices=["MAIN", "INTERNAL"])
    parser.add_argument("--start-nonce", type=int)
    args = parser.parse_args()
    network_type = NetworkType.MAIN if args.network == "MAIN" else NetworkType.INTERNAL
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        to_reward_file(args.season, args.path, network_type, args.start_nonce)
    )
    loop.close()
