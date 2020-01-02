import asyncio
import os

import httpx
import regex

from .base import BitmexBase
from .constants import (
    ALL_KEYS,
    CONNECTION_KEY,
    MAX_ITEMS,
    PYTEST_CURRENT_TEST,
    SYMBOL_READY_KEY,
)
from .lib import (
    get_api_stream_key,
    get_trade_stream_key,
    get_trades,
    get_websocket_cursor_key,
    get_websocket_stream_key,
)


class BitmexRawProcessor(BitmexBase):
    async def set_all_keys(self, symbols):
        keys = [SYMBOL_READY_KEY]
        for symbol in symbols:
            keys += [get_trade_stream_key(symbol), get_api_stream_key(symbol)]
        await self.redis.sadd(ALL_KEYS, *keys)

    async def main(self, symbols=[]):
        await self.set_all_keys(symbols)
        try:
            await self.process(symbols)
        except asyncio.CancelledError:
            self.stop_execution = True

    async def process(self, symbols):
        if len(symbols):
            await self.set_all_keys(symbols)
            # Not ready, websocket will buffer.
            await self.redis.delete(SYMBOL_READY_KEY)
            while not self.stop_execution:
                for symbol in symbols:
                    is_connected = await self.is_connected()
                    is_ready = await self.is_ready(symbol)
                    # Ready.
                    if is_connected and is_ready:
                        await self.process_symbol(symbol)
                    # Fetch missing trades from API.
                    else:
                        # Not ready.
                        await self.redis.srem(SYMBOL_READY_KEY, symbol)
                        # Stop polling on reconnect.
                        while not is_ready:
                            is_ready = await self.is_ready(symbol)
                            await self.fetch_missing(symbol)

    async def is_connected(self):
        connected = await self.redis.get(CONNECTION_KEY)
        return int(connected)

    async def is_ready(self, symbol):
        ready = await self.redis.sismember(SYMBOL_READY_KEY, symbol)
        return int(ready)

    async def process_symbol(self, symbol):
        trade_stream_key = get_trade_stream_key(symbol)
        websocket_stream_key = get_websocket_stream_key(symbol)
        websocket_cursor_key = get_websocket_cursor_key(symbol)
        api_stream_key = get_api_stream_key(symbol)
        api_data = await self.read_stream(api_stream_key)
        websocket_cursor = await self.redis.get(websocket_cursor_key)
        websocket_data = await self.read_stream(
            websocket_stream_key, start=websocket_cursor
        )
        if api_data:
            # Should extend to last websocket.
            all_data = api_data + websocket_data
            # Sort by timestamp.
            all_data.sort(key=lambda data: data[2]["timestamp"])
        else:
            all_data = websocket_data
        # Copy to trade stream and delete buffer stream key.
        for stream_key, redis_id, trade in all_data:
            # No longer necessary.
            del trade["symbol"]
            # Standardize
            trade["volume"] = trade["size"]
            del trade["size"]
            await self.redis.xadd(trade_stream_key, trade)
        cursor = websocket_data[-1][1] if len(websocket_data) else None
        if cursor:
            cursor_key = get_websocket_cursor_key(symbol)
            await self.redis.set(cursor_key, cursor)
        # Reclaim memory.
        await self.redis.xtrim(api_stream_key, 0)
        await self.redis.xtrim(websocket_stream_key, MAX_ITEMS)
        if PYTEST_CURRENT_TEST in os.environ:
            self.stop_execution = True

    async def fetch_missing(self, symbol):
        data = await self.api_request(symbol)
        if isinstance(data, list) and len(data):
            filtered = await self.filter_data(symbol, data)
            if len(filtered):
                api_stream_key = get_api_stream_key(symbol)
                for trade in get_trades(filtered):
                    await self.redis.xadd(api_stream_key, trade)
            # May have exceeded 1000 message limit.
            elif not len(filtered) and len(data) == 1000:
                await self.fetch_missing(symbol)
            else:
                # Ready, will merge streams next iteration.
                await self.redis.sadd(SYMBOL_READY_KEY, symbol)
        else:
            # Ready, will merge streams next iteration.
            await self.redis.sadd(SYMBOL_READY_KEY, symbol)

    async def api_request(self, symbol):
        params = await self.get_api_request_params(symbol)
        if params:
            url = f"https://www.bitmex.com/api/v1/trade?{params}"
            r = await httpx.get(url)
            data = r.json()
            if "error" in data:
                delay = 0
                name = data["error"].get("name", None)
                message = data["error"].get("message", None)
                if name and message:
                    re = regex.compile(r".+(\d).+")
                    match = re.match(message)
                    if match:
                        delay = int(match.group(1))
                # Backoff
                await asyncio.sleep(delay)
                # Retry
                data = await self.api_request(symbol)
            return data

    async def get_api_request_params(self, symbol):
        filter_params = await self.get_api_request_filter_params(symbol)
        if filter_params:
            return f"symbol={symbol}&count=1000&{filter_params}"

    async def get_api_request_filter_params(self, symbol):
        # Keys
        trade_stream_key = get_trade_stream_key(symbol)
        websocket_stream_key = get_websocket_stream_key(symbol)
        api_stream_key = get_api_stream_key(symbol)
        # Last
        last_trade = await self.read_last(trade_stream_key)
        # First
        first_websocket = await self.read_first(websocket_stream_key)
        # Last
        last_api = await self.read_last(api_stream_key)
        # Defaults
        start = None
        stop = None
        # Server initialized, symbol is ready.
        if not last_trade and not first_websocket and not last_api:
            await self.redis.sadd(SYMBOL_READY_KEY, symbol)
        # After one iteration, both buffers exist, so check first.
        elif last_api and first_websocket:
            if last_api["timestamp"] <= first_websocket["timestamp"]:
                # From last API call to last_websocket.
                start = last_api["timestamp"]
                stop = first_websocket["timestamp"]
        # Websocket reconnected, fetch missing.
        elif last_trade and first_websocket:
            if last_trade["timestamp"] <= first_websocket["timestamp"]:
                # From last trade to last websocket.
                start = last_trade["timestamp"]
                stop = first_websocket["timestamp"]
        # Websocket not connected, so buffer api.
        elif last_trade:
            start = last_trade["timestamp"]
        # Continue buffering.
        elif last_trade and last_api:
            start = last_api["timestamp"]
        if start:
            start = f"startTime={start}"
        if stop:
            stop = f"stopTime={stop}"
        return "&".join([param for param in (start, stop) if param])

    async def filter_data(self, symbol, data):
        # Keys
        trade_stream_key = get_trade_stream_key(symbol)
        websocket_stream_key = get_websocket_stream_key(symbol)
        api_stream_key = get_api_stream_key(symbol)
        # Read data.
        trade_data = await self.read_stream(trade_stream_key)
        websocket_data = await self.read_stream(websocket_stream_key)
        api_data = await self.read_stream(api_stream_key)
        # trdMatchIds
        trade_trdMatchIDs = [d[2]["trdMatchID"] for d in trade_data]
        api_trdMatchIDs = [d[2]["trdMatchID"] for d in api_data]
        websocket_trdMatchIDs = [d[2]["trdMatchID"] for d in websocket_data]
        # Trades may have same timestamp, not trdMatchID.
        all_data = []
        first_websocket = websocket_data[0][2] if len(websocket_data) else None
        for d in data:
            trade_exists = d["trdMatchID"] in trade_trdMatchIDs
            api_exists = d["trdMatchID"] in api_trdMatchIDs
            websocket_exists = d["trdMatchID"] in websocket_trdMatchIDs
            if not trade_exists and not api_exists and not websocket_exists:
                if first_websocket:
                    if d["timestamp"] <= first_websocket["timestamp"]:
                        all_data.append(d)
                else:
                    all_data.append(d)
        return all_data
