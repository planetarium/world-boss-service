import argparse
import asyncio
from typing import Tuple, List

from world_boss.app.raid import check_total_amount


async def main(file_path: str, currencies: List[Tuple[str, int]]):
    await check_total_amount(file_path, currencies)


class ParseDict(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        d = getattr(namespace, self.dest) or {}

        if values:
            for item in values:
                split_items = item.split("=", 1)
                key = split_items[
                    0
                ].strip()  # we remove blanks around keys, as is logical
                value = split_items[1]

                d[key] = value

        setattr(namespace, self.dest, d)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file-path")
    parser.add_argument(
        "--currency",
        metavar="KEY=VALUE",
        nargs="+",
        action=ParseDict,
    )
    args = parser.parse_args()
    loop = asyncio.get_event_loop()
    currencies = [(k, int(args.currency[k])) for k in args.currency]
    loop.run_until_complete(main(args.file_path, currencies))
    loop.close()
