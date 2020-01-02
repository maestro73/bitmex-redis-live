import asyncio
import os

import httpx
import pendulum
import pytest

from afml_server.constants import CONNECTION_KEY
from afml_server.lib import (
    get_api_stream_key,
    get_redis,
    get_trade_stream_key,
    get_trades,
    get_websocket_stream_key,
    set_environment,
)
from afml_server.raw_processor import BitmexRawProcessor
from afml_server.symbols import XBTUSD

pytestmark = pytest.mark.asyncio


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
def raw_processor(redis):
    return BitmexRawProcessor(redis)


async def mock_client(redis, symbol, minutes_ago=1, expected=None):
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
        data = await mock_client(
            redis, symbol, minutes_ago=minutes_ago, expected=expected
        )
    return data


async def mock_reconnect(redis):
    while True:
        api_stream_key = get_api_stream_key(XBTUSD)
        # Wait for API request.
        last = await read_last(redis, api_stream_key)
        if last:
            await set_connected(redis, 1)
            break


async def set_connected(redis, connected):
    assert connected in (0, 1)
    await redis.set(CONNECTION_KEY, connected)


async def read_stream(redis, stream_key):
    data = await redis.xrange(stream_key)
    return [d[1] for d in data]


async def read_last(redis, stream_key):
    data = await redis.xrevrange(stream_key, count=1)
    return data[0] if data else None


async def get_data(redis, symbol, expected=None):
    data = await mock_client(redis, symbol, expected=expected)
    return data


async def add_data_to_stream(redis, stream_key, data):
    data = [data] if isinstance(data, dict) else data
    for trade in get_trades(data):
        await redis.xadd(stream_key, trade)


async def assert_data(redis, data, expected=None):
    expected = expected or len(data)
    trade_stream_key = get_trade_stream_key(XBTUSD)
    trades = await read_stream(redis, trade_stream_key)
    # Maybe more.
    assert len(trades) >= expected
    assert_trades(data, trades)


def assert_trades(data, trades):
    for (original, trade) in zip(data, trades):
        assert original["trdMatchID"] == trade["trdMatchID"]
        assert original["timestamp"] == trade["timestamp"]


async def test_init(event_loop, redis, raw_processor):
    await set_connected(redis, 1)
    data = await get_data(redis, XBTUSD, expected=1)
    assert len(data) >= 1
    stream_key = get_websocket_stream_key(XBTUSD)
    await add_data_to_stream(redis, stream_key, data)
    await asyncio.ensure_future(raw_processor.main(symbols=[XBTUSD]))
    await assert_data(redis, data)
    await cleandb(redis)


async def test_api_and_websocket(redis, raw_processor):
    await set_connected(redis, 1)
    data = await get_data(redis, XBTUSD, expected=3)
    assert len(data) >= 3
    api_data = data[0]
    websocket_data = data[-1]
    # Maybe equal.
    assert websocket_data["timestamp"] >= api_data["timestamp"]
    api_stream_key = get_api_stream_key(XBTUSD)
    websocket_stream_key = get_websocket_stream_key(XBTUSD)
    await add_data_to_stream(redis, api_stream_key, api_data)
    await add_data_to_stream(redis, websocket_stream_key, websocket_data)
    await asyncio.ensure_future(raw_processor.main(symbols=[XBTUSD]))
    await assert_data(redis, data)
    await cleandb(redis)


async def test_trade_and_websocket(redis, raw_processor):
    await set_connected(redis, 1)
    data = await get_data(redis, XBTUSD, expected=3)
    assert len(data) >= 3
    trade_data = data[0]
    websocket_data = data[-1]
    # Maybe equal.
    assert websocket_data["timestamp"] >= trade_data["timestamp"]
    trade_stream_key = get_trade_stream_key(XBTUSD)
    websocket_stream_key = get_websocket_stream_key(XBTUSD)
    await add_data_to_stream(redis, trade_stream_key, trade_data)
    await add_data_to_stream(redis, websocket_stream_key, websocket_data)
    await asyncio.ensure_future(raw_processor.main(symbols=[XBTUSD]))
    await assert_data(redis, data)
    await cleandb(redis)


async def test_reconnect(redis, raw_processor):
    await set_connected(redis, 1)
    data = await get_data(redis, XBTUSD, expected=1)
    assert len(data) >= 1
    trade_stream_key = get_trade_stream_key(XBTUSD)
    await add_data_to_stream(redis, trade_stream_key, data)
    reconnect_task = asyncio.ensure_future(mock_reconnect(redis))
    raw_processor_task = asyncio.ensure_future(raw_processor.main(symbols=[XBTUSD]))
    done, pending = await asyncio.wait(
        [reconnect_task, raw_processor_task], return_when=asyncio.FIRST_COMPLETED
    )
    trade_stream_key = get_trade_stream_key(XBTUSD)
    trades = await read_stream(redis, trade_stream_key)
    assert len(trades) >= 1
    await cleandb(redis)
