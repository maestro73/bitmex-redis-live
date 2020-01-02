import asyncio
from collections import OrderedDict
from math import floor

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
            agg["timestamp"] = trade["timestamp"]
            agg["size"] = int(agg["size"]) + int(trade["size"])
            agg["imbalance"] = int(agg["imbalance"]) + self.get_imbalance(trade)
            agg["homeNotional"] = float(agg["homeNotional"]) + float(
                trade["homeNotional"]
            )
            if price >= high_thresh or price <= low_thresh:
                agg_stream_key = get_aggregate_stream_key(symbol)
                agg = OrderedDict(
                    [
                        ("timestamp", agg["timestamp"]),
                        ("price", price),
                        ("size", agg["size"]),
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
                        "price": agg["price"],
                        "size": 0,
                        "imbalance": 0,
                        "homeNotional": 0,
                    },
                )
            else:
                await self.redis.hmset_dict(agg_hash_key, agg)
        else:
            trade["price"] = floor(float(trade["price"]))
            trade["imbalance"] = self.get_imbalance(trade)
            await self.reset_aggregate_hash(symbol, trade)

    def get_imbalance(self, trade):
        tick_direction = trade["tickDirection"]
        direction = 1 if tick_direction in ("PlusTick", "ZeroPlusTick") else -1
        return int(trade["size"]) * direction

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
