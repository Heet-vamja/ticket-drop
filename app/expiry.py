import asyncio
import time

from . import holds


async def sweep_once() -> list[str]:
    """Pop holds whose deadline has passed; the Redis TTL already freed the seat, this tidies the index
    and is where we'd notify the user and re-open the seat on a seat map."""
    expired = await holds.redis.zrangebyscore(holds.DEADLINES, 0, time.time())
    for member in expired:
        event_id, seat_id = member.split(":")
        if not await holds.redis.exists(holds.seat_key(int(event_id), int(seat_id))):
            await holds.redis.zrem(holds.DEADLINES, member)
    return expired


async def run(interval: float = 1.0) -> None:
    while True:
        try:
            await sweep_once()
        except Exception:  # keep the worker alive through Redis blips
            pass
        await asyncio.sleep(interval)
