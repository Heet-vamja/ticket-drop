"""M3 waiting room. Redis sorted set; score = assigned position.

Fairness: everyone who joins in the first ENTRY_WINDOW_SECONDS gets a random score in [0, 1), so network
speed doesn't matter. Later arrivals get 1 + seconds-since-window-closed, i.e. FIFO behind the lottery.
"""
import asyncio
import json
import random
import time

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from . import antibot, metrics
from .config import ADMIT_INTERVAL_SECONDS, ENTRY_WINDOW_SECONDS, GATE
from .holds import redis

router = APIRouter(prefix="/queue")
QUEUE = "queue:z"
OPENED_AT = "queue:opened_at"


async def _window_end() -> float | None:
    opened = await redis.get(OPENED_AT)
    return float(opened) + ENTRY_WINDOW_SECONDS if opened else None


@router.post("/open")
async def open_queue():
    """Start the drop: resets the queue and starts the randomized entry window."""
    await redis.delete(QUEUE)
    await redis.set(OPENED_AT, time.time())
    return {"window_seconds": ENTRY_WINDOW_SECONDS}


async def position_of(user: str) -> dict:
    rank = await redis.zrank(QUEUE, user)
    if rank is None:
        token = await redis.get(f"admit:{user}")
        if token:
            return {"status": "admitted", "user": user, "token": token}
        raise HTTPException(404, "not in queue")
    rate = GATE["batch"] / ADMIT_INTERVAL_SECONDS  # users admitted per second (live, adaptive)
    return {
        "status": "queued",
        "user": user,
        "position": rank + 1,
        "ahead": rank,
        "total": await redis.zcard(QUEUE),
        "eta_seconds": round(rank / rate) if rate else None,
    }


async def join(user: str):
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
            except HTTPException:
                yield "event: gone\ndata: {}\n\n"
                return
            yield f"data: {json.dumps(data)}\n\n"
            if data["status"] == "admitted":
                return
            await asyncio.sleep(2 + random.random())  # jitter so connections don't pulse in sync

    return StreamingResponse(stream(), media_type="text/event-stream")


@router.get("/stats")
async def stats():
    return {"queued": await redis.zcard(QUEUE), "window_end": await _window_end()}
