import os
import time

import redis.asyncio as aioredis

from .config import HOLD_TTL_SECONDS

redis = aioredis.from_url(os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0"), decode_responses=True, max_connections=1000,
    socket_timeout=1.0, socket_connect_timeout=1.0)

DEADLINES = "holds:deadlines"  # sorted set: member "event:seat", score = deadline (epoch seconds)


def seat_key(event_id: int, seat_id: int) -> str:
    return f"seat:{event_id}:{seat_id}"


# Delete only if we still own the hold (compare-and-delete must be atomic).
_RELEASE = redis.register_script(
    "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end"
)


async def try_hold(event_id: int, seat_id: int, user: str) -> bool:
    """SET NX EX: atomic across machines, expiry built in. Returns True if we got the seat."""
    ok = await redis.set(seat_key(event_id, seat_id), user, nx=True, ex=HOLD_TTL_SECONDS)
    if ok:
        await redis.zadd(DEADLINES, {f"{event_id}:{seat_id}": time.time() + HOLD_TTL_SECONDS})
    return bool(ok)


async def owns_hold(event_id: int, seat_id: int, user: str) -> bool:
    return await redis.get(seat_key(event_id, seat_id)) == user


async def release(event_id: int, seat_id: int, user: str) -> bool:
    released = bool(await _RELEASE(keys=[seat_key(event_id, seat_id)], args=[user]))
    await redis.zrem(DEADLINES, f"{event_id}:{seat_id}")
    return released


async def extend_hold(event_id: int, seat_id: int, user: str, seconds: int) -> bool:
    """Grace period for a payment that lands right at expiry; only works if nobody grabbed the seat."""
    if await owns_hold(event_id, seat_id, user):
        await redis.expire(seat_key(event_id, seat_id), seconds)
        return True
    # Key already expired: re-take it only if still free.
    return bool(await redis.set(seat_key(event_id, seat_id), user, nx=True, ex=seconds))
