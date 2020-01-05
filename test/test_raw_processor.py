import asyncio
import os

import httpx
import pendulum
import pytest

from bitmex_redis_live.lib import get_redis, get_trades, set_environment, trade_stream
from bitmex_redis_live.raw_processor import BitmexRawProcessor
from bitmex_redis_live.symbols import XBTUSD

pytestmark = pytest.mark.asyncio

TRADE_STREAM = trade_stream(XBTUSD)


@pytest.fixture
async def setenv(redis):
    # Setup
    for key, value in set_environment():
        os.environ[key] = value


@pytest.fixture
async def redis():
    r = await get_redis()
    await r.flushdb()
    return r


async def cleandb(redis):
    await redis.flushdb()
    redis.close()
    await redis.wait_closed()


@pytest.fixture
def raw_processor(redis, symbols=[XBTUSD]):
    return BitmexRawProcessor(redis, symbols)


async def mock_client(symbol, minutes_ago=1, expected=None):
    now = pendulum.now("UTC")
    count = 1000
    if expected:
        assert expected <= 1000
    start_time = now.subtract(minutes=minutes_ago).naive()
    params = f"symbol={symbol}&count={count}&startTime={start_time}"
    url = f"https://www.bitmex.com/api/v1/trade?{params}"
    r = await httpx.get(url)
    data = r.json()
    if expected and len(data) < expected:
        minutes_ago += 1
        data = await mock_client(symbol, minutes_ago=minutes_ago, expected=expected)
    return data


async def read_stream(redis):
    data = await redis.xrange(TRADE_STREAM)
    return [d[1] for d in data]


async def read_last(redis):
    data = await redis.xrevrange(TRADE_STREAM, count=1)
    return data[0] if data else None


async def get_data(symbol, expected=None):
    data = await mock_client(symbol, expected=expected)
    return data


async def add_data_to_stream(redis, data):
    data = [data] if isinstance(data, dict) else data
    for trade in get_trades(data):
        await redis.xadd(TRADE_STREAM, trade)


async def assert_data(redis, data, expected=None):
    expected = expected or len(data)
    trades = await read_stream(redis)
    # Maybe more.
    assert len(trades) >= expected
    assert_trades(data, trades)


def assert_trades(data, trades):
    for (original, trade) in zip(data, trades):
        assert original["trdMatchID"] == trade["trdMatchID"]
        assert original["timestamp"] == trade["timestamp"]


async def test_init(event_loop, redis, raw_processor):
    data = await get_data(XBTUSD, expected=1)
    assert len(data) >= 1
    for trade in data:
        await asyncio.ensure_future(raw_processor.main(trade))
    await assert_data(redis, data)
    await cleandb(redis)


async def test_api_and_queue(redis, raw_processor):
    data = await get_data(XBTUSD, expected=3)
    assert len(data) >= 3
    api_data = data[0]
    queue_data = data[-1]
    # Maybe equal.
    assert queue_data["timestamp"] >= api_data["timestamp"]
    raw_processor.api[XBTUSD] = api_data
    raw_processor.queue[XBTUSD] = queue_data
    await asyncio.ensure_future(raw_processor.fetch_missing())
    await asyncio.ensure_future(raw_processor.process(XBTUSD))
    await assert_data(redis, data)
    await cleandb(redis)


async def test_trade_and_queue(redis, raw_processor):
    data = await get_data(XBTUSD, expected=3)
    assert len(data) >= 3
    trade_data = data[0]
    queue_data = data[-1]
    raw_processor.queue[XBTUSD].append(queue_data)
    # Maybe equal.
    assert queue_data["timestamp"] >= trade_data["timestamp"]
    await add_data_to_stream(redis, trade_data)
    await asyncio.ensure_future(raw_processor.fetch_missing())
    await asyncio.ensure_future(raw_processor.process(XBTUSD))
    await assert_data(redis, data)
    await cleandb(redis)
