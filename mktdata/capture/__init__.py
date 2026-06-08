"""Live market-data capture. Adapters register here so the CLI is table-driven;
adding an exchange/datatype is a new adapter plus one registry entry."""

from .base import ExchangeCapture, run
from .binance import BinanceSpotOrderbook
from .raw_writer import RawWriter

# exchange -> market -> datatype -> adapter class
EXCHANGES = {
    "binance": {"spot": {"orderbook": BinanceSpotOrderbook}},
}

__all__ = ["ExchangeCapture", "run", "RawWriter", "EXCHANGES"]
