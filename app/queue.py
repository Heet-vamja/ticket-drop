"""M3 waiting room. Redis sorted set; score = assigned position.

Fairness: everyone who joins in the first ENTRY_WINDOW_SECONDS gets a random score in [0, 1), so network
speed doesn't matter. Later arrivals get 1 + seconds-since-window-closed, i.e. FIFO behind the lottery.
"""
import asyncio
import json
import random
import time

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse

from . import admission, antibot, booking, metrics
from .auth import require_admin
from .config import ENTRY_WINDOW_SECONDS
from .holds import redis

router = APIRouter(prefix="/queue")
QUEUE = "queue:z"
OPENED_AT = "queue:opened_at"


async def _window_end() -> float | None:
    opened = await redis.get(OPENED_AT)
    return float(opened) + ENTRY_WINDOW_SECONDS if opened else None


@router.post("/open", dependencies=[Depends(require_admin)])
async def open_queue():
    """Start the drop: resets the queue and starts the randomized entry window."""
    await redis.delete(QUEUE)
    await redis.set(OPENED_AT, time.time())
    return {"window_seconds": ENTRY_WINDOW_SECONDS}


def poll_interval(ahead: int) -> float:
    """Backpressure on clients, by distance from the front: people about to be admitted check every ~2s (so a
    freed slot isn't left idle), people deep in the queue check rarely. 10k waiters all polling every 2s would be
    5k req/s of pure noise."""
    return min(30.0, max(2.0, ahead / 50))


async def position_of(user: str) -> dict:
    rank = await redis.zrank(QUEUE, user)
    if rank is None:
        token = await redis.get(f"admit:{user}")
        if token:
            return {"status": "admitted", "user": user, "token": token}
        raise HTTPException(404, "not in queue")
    if await booking.sold_out():
        raise HTTPException(410, "sold out")  # don't keep 100k people waiting for seats that no longer exist
    rate = admission.stats["rate"]  # users/second the gate is actually admitting right now
    total = await redis.zcard(QUEUE)
    return {
        "status": "queued",
        "user": user,
        "position": rank + 1,
        "ahead": rank,
        "total": total,
        "poll_after": round(poll_interval(rank), 1),
        "eta_seconds": round(rank / rate) if rate > 0.05 else None,  # None = gate is full, can't estimate
    }


async def join(user: str):
    if await booking.sold_out():
        raise HTTPException(410, "sold out")  # load shedding: shed cheaply, no queue entry
    end = await _window_end()
    if end is None:
        raise HTTPException(409, "drop has not opened")
    now = time.time()
    score = random.random() if now < end else 1 + (now - end)
    await redis.zadd(QUEUE, {user: score}, nx=True)  # NX: re-joining keeps your place
    return await position_of(user)


@router.get("/challenge")
async def challenge(request: Request, user: str):
    await antibot.throttle(request)
    return await antibot.issue_challenge(user)


@router.post("/join")
async def join_endpoint(request: Request, user: str, solution: str | None = None):
    await antibot.throttle(request, account=user)
    await antibot.require_pow(user, solution)
    result = await join(user)
    await metrics.incr("joined")
    return result


@router.get("/position")
async def position(request: Request, user: str):
    await antibot.throttle(request)
    return await position_of(user)


@router.get("/events")
async def events(user: str):
    """SSE: pushes position/ETA until the client disconnects. Cheaper than every client polling."""

    async def stream():
        while True:
            try:
                data = await position_of(user)
            except HTTPException as e:
                yield f"event: {'soldout' if e.status_code == 410 else 'gone'}\ndata: {{}}\n\n"
                return
            yield f"data: {json.dumps(data)}\n\n"
            if data["status"] == "admitted":
                return
            await asyncio.sleep(poll_interval(data["ahead"]) * (0.8 + 0.4 * random.random()))  # scaled + jittered so connections don't pulse in sync

    return StreamingResponse(stream(), media_type="text/event-stream")


@router.get("/stats")
async def stats():
    return {"queued": await redis.zcard(QUEUE), "window_end": await _window_end()}
