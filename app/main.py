import asyncio
import time
from contextlib import asynccontextmanager

import redis.exceptions
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from . import admin, admission, booking, expiry, metrics, naive, queue, shop
from .db import init_db


@asynccontextmanager
async def lifespan(_: FastAPI):
    await init_db()
    tasks = [asyncio.create_task(expiry.run()), asyncio.create_task(admission.run()),
             asyncio.create_task(metrics.monitor_loop_lag())]
    yield
    for t in tasks:
        t.cancel()


app = FastAPI(title="ticket-drop", lifespan=lifespan)


def _group(path: str) -> str | None:
    if path.startswith("/admin") or path == "/metrics":
        return None  # observing the system shouldn't distort what we observe
    if path.startswith("/queue/events"):
        return "sse"
    if path.startswith("/queue"):
        return "queue"
    if path.startswith(("/shop", "/v2")):
        return "booking"
    return "other"


@app.middleware("http")
async def telemetry(request: Request, call_next):
    group = _group(request.url.path)
    if group is None or group == "sse":  # SSE responses are long-lived; time only the handshake
        if group:
            metrics.hit(group)
        return await call_next(request)
    started = time.perf_counter()
    response = await call_next(request)
    metrics.hit(group)
    if group == "booking":
        metrics.record_latency((time.perf_counter() - started) * 1000)
    return response


@app.exception_handler(redis.exceptions.RedisError)
async def redis_down(_: Request, __: redis.exceptions.RedisError):
    """Fail closed: if Redis (holds/queue) is unreachable we refuse new work rather than guess.
    Sold seats stay safe because the Postgres unique constraint is the final authority."""
    await metrics.count_degraded()
    return JSONResponse({"detail": "temporarily unavailable, please retry"}, status_code=503, headers={"Retry-After": "2"})


@app.get("/metrics")
async def prometheus():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


app.include_router(naive.router)
app.include_router(booking.router)
app.include_router(queue.router)
app.include_router(shop.router)
app.include_router(admin.router)
