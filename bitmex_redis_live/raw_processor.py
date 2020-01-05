import datetime
import time

import httpx
import regex

from .base import BitmexBase
from .constants import ALL_KEYS
from .lib import trade_key, trade_stream


class BitmexRawProcessor(BitmexBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.trades = {symbol: [] for symbol in self.symbols}
        self.queue = {symbol: [] for symbol in self.symbols}
        self.api = {symbol: [] for symbol in self.symbols}
        self.ready = {symbol: False for symbol in self.symbols}
        self.next_api_call = time.time()

    def set_not_ready(self):
        for symbol in self.symbols:
            self.ready[symbol] = False

    async def set_all_keys(self):
        streams = [trade_stream(symbol) for symbol in self.symbols]
        keys = [trade_key(symbol) for symbol in self.symbols]
        all_keys = streams + keys
        await self.redis.sadd(ALL_KEYS, *all_keys)

    async def main(self, trade):
        symbol = trade.pop("symbol")
        if self.ready[symbol]:
            self.trades[symbol].append(trade)
            await self.process(symbol)
        else:
            self.queue[symbol].append(trade)

    async def process(self, symbol):
        while len(self.trades[symbol]):
            trade = self.trades[symbol].pop(0)
            await self.redis.xadd(trade_stream(symbol), trade)
            await self.redis.set(trade_key(symbol), trade["trdMatchID"])

    async def fetch_missing(self):
        for symbol in self.symbols:
            if not self.ready[symbol]:
                if time.time() > self.next_api_call:
                    await self.fetch_missing_symbol(symbol)

    async def fetch_missing_symbol(self, symbol):
        queue_data = self.queue[symbol]
        api_data = self.api[symbol]
        now = datetime.datetime.utcnow()
        # Params
        last_trade = await self.read_last(trade_stream(symbol))
        last_api = api_data[-1] if len(api_data) else None
        # Start time.
        if last_trade:
            start_time = last_trade["timestamp"]
        elif last_api:
            start_time = last_api["timestamp"]
        else:
            start_time = datetime.datetime.combine(
                now.date(), datetime.datetime.min.time()
            ).isoformat()
        # Stop time.
        first_queue = queue_data[0] if len(queue_data) else None
        if first_queue:
            stop_time = first_queue["timestamp"]
        else:
            stop_time = now.isoformat()
        # API request.
        if start_time <= stop_time:
            data, ok = await self.api_request(symbol, start_time, stop_time)
            api_data += data
        else:
            ok = True
        # Cleanup
        if ok:
            if len(api_data):
                filtered_api_data = await self.filter_api_data(symbol, api_data)
                if len(filtered_api_data):
                    all_data = filtered_api_data + queue_data
                    all_data.sort(key=lambda x: x["timestamp"])
                    self.trades[symbol] = all_data
                self.queue = []
                self.api[symbol] = []
            self.ready[symbol] = ok

    async def api_request(self, symbol, start_time, stop_time):
        params = (
            f"symbol={symbol}&count=1000&startTime={start_time}&stopTime={stop_time}"
        )
        url = f"https://www.bitmex.com/api/v1/trade?{params}"
        r = await httpx.get(url)
        data = r.json()
        if "error" in data:
            name = data["error"].get("name", None)
            message = data["error"].get("message", None)
            if name and message:
                re = regex.compile(r".+(\d).+")
                match = re.match(message)
                if match:
                    delay = int(match.group(1))
                    self.next_api_call = time.time() + delay
            return [], False
        elif len(data) == 1000:
            start_time = data[-1]["timestamp"]
            data += self.api_request(symbol, start_time, stop_time)
        return data, len(data) == 0

    async def filter_api_data(self, symbol, api_data):
        queue_data = self.queue[symbol]
        queue_data = await self.filter_trdMatchID(symbol, queue_data, reverse=True)
        api_data = await self.filter_trdMatchID(symbol, api_data)
        api_data_trdMatchIDs = []
        queue_trdMatchIds = [q["trdMatchID"] for q in queue_data]
        filtered = []
        for trade in api_data:
            matchID = trade["trdMatchID"]
            api_exists = matchID in api_data_trdMatchIDs
            queue_exists = matchID in queue_trdMatchIds
            if not api_exists and not queue_exists:
                filtered.append(trade)
            api_data_trdMatchIDs.append(matchID)
        return filtered

    async def filter_trdMatchID(self, symbol, data, reverse=False):
        trdMatchID = await self.redis.get(trade_key(symbol))
        if trdMatchID:
            trdMatchIDs = [d["trdMatchID"] for d in data]
            try:
                index = trdMatchIDs.index(trdMatchID)
            except ValueError:
                return data
            else:
                if reverse:
                    previous_index = index - 1
                    if previous_index >= 0:
                        return data[:previous_index]
                    else:
                        return []
                else:
                    next_index = index + 1
                    return data[next_index:]
        else:
            return data
