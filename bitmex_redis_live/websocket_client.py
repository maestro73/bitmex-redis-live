import asyncio

import ujson
import websockets

from .base import BitmexBase
from .lib import get_trades
from .raw_processor import BitmexRawProcessor


class BitmexWebsocketClient(BitmexBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.raw_processor = BitmexRawProcessor(*args, **kwargs)

    async def main(self):
        try:
            await self.raw_processor.set_all_keys()
            await self.open_websocket()
        except asyncio.CancelledError:
            self.stop_execution = True

    async def open_websocket(self):
        if len(self.symbols):
            query = ",".join(self.symbols)
            endpoint = f"wss://www.bitmex.com/realtime?subscribe=trade:{query}"
            async with websockets.connect(endpoint) as websocket:
                while not self.stop_execution:
                    try:
                        msg = websocket.messages.get_nowait()
                    except asyncio.queues.QueueEmpty:
                        await self.raw_processor.fetch_missing()
                    else:
                        msg = await websocket.recv()
                        msg = ujson.loads(msg)
                        table = msg.get("table", None)
                        if table == "trade":
                            data = msg.get("data", [])
                            for trade in get_trades(data):
                                await self.raw_processor.main(trade)
            # Websocket closed, reconnect.
            if not self.stop_execution:
                await self.raw_processor.set_not_ready()
                await self.open_websocket()
