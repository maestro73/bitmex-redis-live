import asyncio

import ujson
import websockets

from .base import BitmexBase
from .constants import ALL_KEYS, CONNECTION_KEY, WEBSOCKET_KEY_SUFFIX
from .lib import get_trades, get_websocket_stream_key


class BitmexWebsocketClient(BitmexBase):
    async def set_all_keys(self, symbols):
        keys = [CONNECTION_KEY]
        keys += [f"{symbol}-{WEBSOCKET_KEY_SUFFIX}" for symbol in symbols]
        await self.redis.sadd(ALL_KEYS, *keys)

    async def main(self, symbols=[]):
        await self.set_all_keys(symbols)
        try:
            await self.open_websocket(symbols)
        except asyncio.CancelledError:
            self.stop_execution = True

    async def open_websocket(self, symbols):
        if len(symbols):
            await self.redis.set(CONNECTION_KEY, 0)
            query = ",".join(symbols)
            endpoint = f"wss://www.bitmex.com/realtime?subscribe=trade:{query}"
            async with websockets.connect(endpoint) as websocket:
                await self.redis.set(CONNECTION_KEY, 1)
                while not self.stop_execution:
                    msg = await websocket.recv()
                    msg = ujson.loads(msg)
                    table = msg.get("table", None)
                    if table == "trade":
                        data = msg.get("data", [])
                        for trade in get_trades(data):
                            symbol = trade["symbol"]
                            # Add trade to buffer stream.
                            websocket_stream_key = get_websocket_stream_key(symbol)
                            await self.redis.xadd(websocket_stream_key, trade)
            # Websocket closed, reconnect.
            if not self.stop_execution:
                await self.redis.set(CONNECTION_KEY, 0)
                await self.open_websocket()
