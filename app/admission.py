"""M4 admission gate: admit users from the queue in small batches, as signed short-lived JWTs.

Backpressure: the number admitted is driven by the booking service's health, not by demand.
  * max_active caps admitted-but-unfinished users (a slot frees when the token expires or the user buys)
  * the batch size shrinks when booking latency rises and grows back when it recovers
"""
import asyncio
import time

import jwt

from . import metrics
from .config import (
    ADMIT_INTERVAL_SECONDS,
    ADMISSION_TTL_SECONDS,
    GATE,
    JWT_SECRET,
    LATENCY_TARGET_MS,
    MAX_BATCH,
    MIN_BATCH,
)
from .holds import redis

QUEUE = "queue:z"
ADMITTED = "admitted:z"  # sorted set: member user, score = token expiry (epoch)


def admit_key(user: str) -> str:
    return f"admit:{user}"


def issue_token(user: str, now: float | None = None) -> str:
    now = now or time.time()
    return jwt.encode({"sub": user, "iat": now, "exp": now + ADMISSION_TTL_SECONDS}, JWT_SECRET, algorithm="HS256")


def verify_token(token: str, verify_exp: bool = True) -> str:
    """Returns the user the token was issued to; raises jwt.InvalidTokenError otherwise."""
    claims = jwt.decode(token, JWT_SECRET, algorithms=["HS256"], options={"verify_exp": verify_exp})
    return claims["sub"]


def adapt() -> None:
    if not GATE["adaptive"]:
        return
    if metrics.ewma_ms > LATENCY_TARGET_MS:
        GATE["batch"] = max(MIN_BATCH, GATE["batch"] // 2)
    elif GATE["batch"] < MAX_BATCH and metrics.ewma_ms < LATENCY_TARGET_MS * 0.6:
        GATE["batch"] = min(MAX_BATCH, GATE["batch"] + 2)


async def admit_once() -> list[str]:
    now = time.time()
    await redis.zremrangebyscore(ADMITTED, 0, now)  # expired tokens free their slot -> next person gets in
    active = await redis.zcard(ADMITTED)
    n = min(GATE["batch"], GATE["max_active"] - active)
    if n <= 0:
        return []
    popped = await redis.zpopmin(QUEUE, n)
    users = [member for member, _ in popped]
    if not users:
        return []
    pipe = redis.pipeline()
    for user in users:
        pipe.set(admit_key(user), issue_token(user, now), ex=ADMISSION_TTL_SECONDS)
        pipe.zadd(ADMITTED, {user: now + ADMISSION_TTL_SECONDS})
    pipe.hincrby(metrics.COUNTERS, "admitted", len(users))
    await pipe.execute()
    await metrics.log_event("admit", f"admitted {len(users)} users (active {active + len(users)}/{GATE['max_active']})")
    return users


async def release_slot(user: str) -> None:
    await redis.zrem(ADMITTED, user)


async def run() -> None:
    while True:
        try:
            adapt()
            await admit_once()
        except Exception:  # Redis blip: skip this tick; fail closed (nobody new gets in)
            pass
        await asyncio.sleep(ADMIT_INTERVAL_SECONDS)
