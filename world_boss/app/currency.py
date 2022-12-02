from typing import Optional, List, Union


class Currency:
    def __init__(
        self, *, decimal_places: int, ticker: str, minters: Optional[List[Union[str, bytes]]] = None
    ) -> None:
        self.decimal_places = decimal_places
        self.minters = [
            minter if isinstance(minter, bytes) else bytes.fromhex(minter)
            for minter in minters
        ] if minters is not None else None
        self.ticker = ticker

    def __eq__(self, other):
        return self.as_dict() == other.as_dict()

    def as_dict(self) -> dict:
        if self.decimal_places > 0xff:
            raise ValueError()

        return {
            "decimalPlaces": self.decimal_places.to_bytes(1, "big"),
            "minters": self.minters,
            "ticker": self.ticker,
        }
