import os
from collections import OrderedDict

import aioredis

from .constants import (
    AGGREGATE_CACHE_KEY_SUFFIX,
    AGGREGATE_CURSOR_SUFFIX,
    AGGREGATE_KEY_SUFFIX,
    API_KEY_SUFFIX,
    TRADE_KEY_SUFFIX,
    WEBSOCKET_CURSOR_SUFFIX,
    WEBSOCKET_KEY_SUFFIX,
)

# MARK_PRICE instrument:XBT,


def set_environment():
    with open("env.yaml", "r") as env:
        for line in env:
            key, value = line.split(": ")
            os.environ[key] = value.rstrip()


async def get_redis():
    url = os.environ.get("REDIS_URL", "localhost?encoding=utf-8")
    password = os.environ.get("REDIS_PASS", None)
    params = f"&password={password}" if password else ""
    redis = await aioredis.create_redis_pool(f"redis://{url}{params}", maxsize=256)
    return redis


def get_trade_stream_key(symbol):
    return f"{symbol}-{TRADE_KEY_SUFFIX}"


def get_websocket_stream_key(symbol):
    return f"{symbol}-{WEBSOCKET_KEY_SUFFIX}"


def get_websocket_cursor_key(symbol):
    return f"{symbol}-{WEBSOCKET_CURSOR_SUFFIX}"


def get_api_stream_key(symbol):
    return f"{symbol}-{API_KEY_SUFFIX}"


def get_aggregate_cursor_key(symbol):
    return f"{symbol}-{AGGREGATE_CURSOR_SUFFIX}"


def get_aggregate_stream_key(symbol):
    return f"{symbol}-{AGGREGATE_KEY_SUFFIX}"


def get_aggregate_hash_key(symbol):
    return f"{symbol}-{AGGREGATE_CACHE_KEY_SUFFIX}"


def get_trades(trades):
    for trade in trades:
        yield OrderedDict(
            [
                ("symbol", trade["symbol"]),
                ("timestamp", trade["timestamp"]),
                ("trdMatchID", trade["trdMatchID"]),
                ("tickDirection", trade["tickDirection"]),
                ("price", trade["price"]),
                ("side", trade["side"]),
                ("size", trade["size"]),
                ("homeNotional", trade["homeNotional"]),
            ]
        )
