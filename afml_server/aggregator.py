import asyncio
from collections import OrderedDict
from math import floor

import pendulum

from .base import BitmexBase
from .constants import ALL_KEYS, MAX_ITEMS
from .lib import (
    get_aggregate_cursor_key,
    get_aggregate_hash_key,
    get_aggregate_stream_key,
    get_trade_stream_key,
)
from .symbols import XBTUSD_AGGREGATE_BY


class BitmexAggregator(BitmexBase):
    async def set_all_keys(self, symbols):
        keys = [get_aggregate_cursor_key(symbol) for symbol in symbols]
        keys += [get_aggregate_stream_key(symbol) for symbol in symbols]
        keys += [get_aggregate_hash_key(symbol) for symbol in symbols]
        await self.redis.sadd(ALL_KEYS, *keys)

    async def main(self, symbols=[]):
        await self.set_all_keys(symbols)
        try:
            await self.aggregate_trades(symbols)
        except asyncio.CancelledError:
            self.stop_execution = True

    async def aggregate_trades(self, symbols):
        if len(symbols):
            await self.set_all_keys(symbols)
            while not self.stop_execution:
                for symbol in symbols:
                    trade_stream_key = get_trade_stream_key(symbol)
                    # Get cursor.
                    cursor_key = get_aggregate_cursor_key(symbol)
                    cursor = await self.redis.get(cursor_key)
                    # Get trades.
                    trades = await self.read_stream(trade_stream_key, start=cursor)
                    for _, redis_id, trade in trades:
                        await self.aggregate_trade(symbol, trade)
                    if len(trades):
                        # Set cursor.
                        cursor = trades[-1][1]
                        cursor_key = get_aggregate_cursor_key(symbol)
                        await self.redis.set(cursor_key, cursor)
                        # Reclaim memory.
                        await self.redis.xtrim(trade_stream_key, MAX_ITEMS)

    async def aggregate_trade(self, symbol, trade):
        agg_hash_key = get_aggregate_hash_key(symbol)
        agg = await self.redis.hgetall(agg_hash_key)
        price = float(trade["price"])
        if len(agg):
            agg_price = float(agg["price"])
            high_thresh = agg_price + XBTUSD_AGGREGATE_BY
            low_thresh = agg_price - XBTUSD_AGGREGATE_BY
            # Increment.
            agg["close"] = trade["timestamp"]
            agg["volume"] = int(agg["volume"]) + int(trade["volume"])
            agg["ticks"] = int(agg["ticks"]) + 1
            agg["imbalance"] = int(agg["imbalance"]) + self.get_imbalance(trade)
            agg["homeNotional"] = float(agg["homeNotional"]) + float(
                trade["homeNotional"]
            )
            if price >= high_thresh or price <= low_thresh:
                agg_stream_key = get_aggregate_stream_key(symbol)
                agg = OrderedDict(
                    [
                        ("timestamp", agg["close"]),
                        ("time", self.get_time(agg)),
                        ("price", price),
                        ("volume", agg["volume"]),
                        ("ticks", agg["ticks"]),
                        ("imbalance", agg["imbalance"]),
                        ("homeNotional", agg["homeNotional"]),
                    ]
                )
                print(agg)
                await self.redis.xadd(agg_stream_key, agg)
                # Reset cache.
                await self.reset_aggregate_hash(
                    symbol,
                    {
                        "open": trade["timestamp"],
                        "close": "",
                        "price": agg["price"],
                        "volume": 0,
                        "ticks": 0,
                        "imbalance": 0,
                        "homeNotional": 0,
                    },
                )
            else:
                await self.redis.hmset_dict(agg_hash_key, agg)
        else:
            trade["open"] = trade["timestamp"]
            trade["close"] = ""
            del trade["timestamp"]
            trade["price"] = floor(float(trade["price"]))
            trade["ticks"] = 1
            trade["imbalance"] = self.get_imbalance(trade)
            await self.reset_aggregate_hash(symbol, trade)

    def get_imbalance(self, data):
        tick_direction = data["tickDirection"]
        direction = 1 if tick_direction in ("PlusTick", "ZeroPlusTick") else -1
        return int(data["volume"]) * direction

    def get_time(self, data):
        assert "open" in data and "close" in data
        open_time = pendulum.parse(data["open"])
        close_time = pendulum.parse(data["close"])
        assert close_time >= open_time
        diff = close_time - open_time
        return diff.total_seconds()

    async def reset_aggregate_hash(self, symbol, data):
        assert isinstance(data, dict)
        agg_hash_key = get_aggregate_hash_key(symbol)
        agg = {}
        for key, value in data.items():
            if key == "price":
                agg["price"] = floor(float(value))
            else:
                agg[key] = value
        await self.redis.hmset_dict(agg_hash_key, agg)
