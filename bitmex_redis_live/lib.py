import os
from collections import OrderedDict

from aredis import StrictRedis

from .constants import TRADE_HASH_KEY_SUFFIX, TRADE_STREAM_SUFFIX

# MARK_PRICE instrument:XBT,


def set_environment():
    with open("env.yaml", "r") as env:
        for line in env:
            key, value = line.split(": ")
            os.environ[key] = value.rstrip()


def get_redis():
    url = os.environ.get("REDIS_URL", "localhost")
    password = os.environ.get("REDIS_PASS", None)
    passwd = f"&password={password}" if password else ""
    params = f"?encoding=utf-8{passwd}"
    return StrictRedis(f"redis://{url}{params}", max_connections=256)


def trade_stream(symbol):
    return f"{symbol}-{TRADE_STREAM_SUFFIX}"


def trade_hash(symbol):
    return f"{symbol}-{TRADE_HASH_KEY_SUFFIX}"


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
