import asyncio
import sys

from world_boss.app.raid import to_reward_file


async def main(raid_id: int, file_path: str) -> int:
    try:
        await to_reward_file(raid_id, file_path)
        return 0
    except Exception:
        return -1


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(int(sys.argv[1]), sys.argv[2]))
    loop.close()
