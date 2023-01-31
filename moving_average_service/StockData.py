from typing import NamedTuple


class EmaData(NamedTuple):
    symbol: str
    timestamp: int
    ema10: float
    ema100: float
    advise: str
