"""M3 waiting room. Redis sorted set; score = assigned position.

Fairness: everyone who joins in the first ENTRY_WINDOW_SECONDS gets a random score in [0, 1), so network
speed doesn't matter. Later arrivals get 1 + seconds-since-window-closed, i.e. FIFO behind the lottery.
"""
import asyncio
import json
import random
import time

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from .config import ADMIT_BATCH, ADMIT_INTERVAL_SECONDS, ENTRY_WINDOW_SECONDS
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
        raise HTTPException(404, "not in queue")
    position = rank + 1
    rate = ADMIT_BATCH / ADMIT_INTERVAL_SECONDS  # users admitted per second
    return {
        "user": user,
        "position": position,
        "ahead": rank,
        "total": await redis.zcard(QUEUE),
        "eta_seconds": round(rank / rate),
    }


@router.post("/join")
async def join(user: str):
    end = await _window_end()
    if end is None:
        raise HTTPException(409, "drop has not opened")
    now = time.time()
    score = random.random() if now < end else 1 + (now - end)
    await redis.zadd(QUEUE, {user: score}, nx=True)  # NX: re-joining keeps your place
    return await position_of(user)


@router.get("/position")
async def position(user: str):
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
            await asyncio.sleep(2 + random.random())  # jitter so connections don't pulse in sync

    return StreamingResponse(stream(), media_type="text/event-stream")


@router.get("/stats")
async def stats():
    return {"queued": await redis.zcard(QUEUE), "window_end": await _window_end()}
