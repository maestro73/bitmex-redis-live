from lib import get_redis


class BitmexBase:
    def __init__(self, redis, symbols):
        self.redis = get_redis()
        self.symbols = symbols
        self.stop_execution = False

    async def read_stream(
        self, stream_key, start=None, stop=None, count=None, reverse=False
    ):
        start = start or "-"
        stop = stop or "+"
        command = "xrange" if not reverse else "xrevrange"
        cmd = getattr(self.redis, command)
        data = await cmd(stream_key, start=start, stop=stop, count=count)
        if start and len(data):
            if data[0][0] == start:
                data = data[1:]
        if stop and len(data):
            if data[-1][0] == stop:
                data = data[:-1]
        return [(stream_key, d[0], d[1]) for d in data]

    async def read_first(self, stream_key):
        data = await self.redis.xrange(stream_key, count=1)
        return data[0][1] if len(data) else None

    async def read_last(self, stream_key):
        data = await self.redis.xrevrange(stream_key, count=1)
        return data[0][1] if len(data) else None
