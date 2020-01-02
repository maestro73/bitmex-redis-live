import uvloop

from afml_server.aggregator import BitmexAggregator
from afml_server.lib import get_redis
from afml_server.raw_processor import BitmexRawProcessor
from afml_server.symbols import XBTUSD
from afml_server.websocket_client import BitmexWebsocketClient


async def main(loop, symbols=[]):
    assert len(symbols)
    redis = await get_redis()
    await redis.flushdb()
    websocket_client = BitmexWebsocketClient(redis)
    raw_processor = BitmexRawProcessor(redis)
    aggregator = BitmexAggregator(redis)
    # Bitmex trades, added to "{symbol}-websocket-buffer" stream.
    websocket_task = asyncio.ensure_future(websocket_client.main(symbols))
    # Bitmex trades, added to "{symbol}-trade" stream, after fetching missing.
    raw_processor_task = asyncio.ensure_future(raw_processor.main(symbols))
    # Bitmex trades, added to "{symbol}-trade-aggregated" stream.
    # For example, XBT trades aggregated to $1.
    # Reduces processing later in pipeline.
    aggregator_task = asyncio.ensure_future(aggregator.main(symbols))
    done, pending = await asyncio.wait(
        [websocket_task, raw_processor_task, aggregator_task],
        return_when=asyncio.FIRST_COMPLETED,
    )
    redis.close()
    await redis.wait_closed()


if __name__ == "__main__":
    import asyncio
    from afml_server.lib import set_environment

    set_environment()
    uvloop.install()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop, symbols=[XBTUSD]))
