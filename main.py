import uvloop

# from bitmex_redis_live.aggregator import BitmexAggregator
from bitmex_redis_live.lib import get_redis

# from bitmex_redis_live.raw_processor import BitmexRawProcessor
from bitmex_redis_live.symbols import XBTUSD
from bitmex_redis_live.websocket_client import BitmexWebsocketClient


async def main(loop, symbols=[]):
    assert len(symbols)
    redis = await get_redis()
    await redis.flushdb()
    websocket_client = BitmexWebsocketClient(redis, symbols)
    # raw_processor = BitmexRawProcessor(redis, symbols)
    # aggregator = BitmexAggregator(redis, symbols)
    # Bitmex trades, added to "{symbol}-websocket-buffer" stream.
    websocket_task = asyncio.ensure_future(websocket_client.main())
    # Bitmex trades, added to "{symbol}-trade" stream, after fetching missing.
    # raw_processor_task = asyncio.ensure_future(raw_processor.main())
    # Bitmex trades, added to "{symbol}-trade-aggregated" stream.
    # For example, XBT trades aggregated to $1.
    # Reduces processing later in pipeline.
    # aggregator_task = asyncio.ensure_future(aggregator.main())
    done, pending = await asyncio.wait(
        [websocket_task], return_when=asyncio.FIRST_COMPLETED
    )
    redis.close()
    await redis.wait_closed()


if __name__ == "__main__":
    import asyncio
    from bitmex_redis_live.lib import set_environment

    set_environment()
    uvloop.install()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop, symbols=[XBTUSD]))
