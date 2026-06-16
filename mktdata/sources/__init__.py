"""Source registry. Maps a CLI token to its Source descriptor."""

from .base import DataType, Source, dates_in_range
from .hlarchive import SOURCE as _HLARCHIVE
from .reservoir import SOURCE as _RESERVOIR

SOURCES = {
    _RESERVOIR.name: _RESERVOIR,      # "hydromancer-reservoir"
    _HLARCHIVE.name: _HLARCHIVE,      # "hyperliquid-archive"
}


def get_source(name):
    try:
        return SOURCES[name]
    except KeyError:
        raise SystemExit(f"unknown source {name!r}; known: {', '.join(SOURCES)}")


__all__ = ["SOURCES", "get_source", "Source", "DataType", "dates_in_range"]
