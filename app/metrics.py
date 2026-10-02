"""Cheap in-process telemetry (single uvicorn worker) + Prometheus exposition.

Per-second request buckets feed the admin dashboard; the booking-latency EWMA drives the adaptive admission gate.
Cross-process facts (events log, throttle counters) live in Redis so the sim subprocess and API agree.
"""
import asyncio
import json
import time
from collections import Counter, deque

from prometheus_client import Counter as PCounter
from prometheus_client import Histogram

from .holds import redis

WINDOW_SECONDS = 120
_buckets: dict[int, Counter] = {}
_latencies: deque[tuple[float, float]] = deque(maxlen=20000)  # (timestamp, ms) for booking requests
ewma_ms = 0.0

P_REQUESTS = PCounter("td_requests_total", "HTTP requests", ["group"])
P_BOOKING_LATENCY = Histogram("td_booking_seconds", "Booking request latency")

EVENT_LOG = "events:log"
COUNTERS = "m:counters"  # redis hash: throttled, rejected, sold, held, admitted, ...


def hit(group: str) -> None:
    now = int(time.time())
    _buckets.setdefault(now, Counter())[group] += 1
    for sec in [s for s in _buckets if s < now - WINDOW_SECONDS]:
        del _buckets[sec]
    P_REQUESTS.labels(group).inc()


def record_latency(ms: float) -> None:
    global ewma_ms
    _latencies.append((time.time(), ms))
    ewma_ms = ms if ewma_ms == 0 else 0.8 * ewma_ms + 0.2 * ms
    P_BOOKING_LATENCY.observe(ms / 1000)


def rps_series(seconds: int = 60) -> list[dict]:
    """Oldest-first list of {t, total, <group>: n}; excludes the in-progress current second."""
    now = int(time.time())
    out = []
    for sec in range(now - seconds, now):
        c = _buckets.get(sec, Counter())
        out.append({"t": sec, "total": sum(c.values()), **c})
    return out


def latency_stats(window: float = 10.0) -> dict:
    cutoff = time.time() - window
    vals = sorted(ms for ts, ms in _latencies if ts >= cutoff)
    if not vals:
        return {"p50": 0.0, "p99": 0.0, "ewma": round(ewma_ms, 1), "n": 0}
    return {
        "p50": round(vals[len(vals) // 2], 1),
        "p99": round(vals[min(len(vals) - 1, int(len(vals) * 0.99))], 1),
        "ewma": round(ewma_ms, 1),
        "n": len(vals),
    }


async def incr(counter: str, by: int = 1) -> None:
    await redis.hincrby(COUNTERS, counter, by)


async def log_event(kind: str, message: str) -> None:
    entry = json.dumps({"t": time.time(), "kind": kind, "msg": message})
    pipe = redis.pipeline()
    pipe.lpush(EVENT_LOG, entry)
    pipe.ltrim(EVENT_LOG, 0, 99)
    await pipe.execute()


loop_lag_ms = 0.0


async def monitor_loop_lag() -> None:
    """How late the event loop wakes us: a direct, honest measure of how overloaded the API process is."""
    global loop_lag_ms
    while True:
        started = time.perf_counter()
        await asyncio.sleep(0.1)
        lag = max(0.0, (time.perf_counter() - started - 0.1) * 1000)
        loop_lag_ms = 0.5 * loop_lag_ms + 0.5 * lag


degraded_total = 0


async def count_degraded() -> None:
    global degraded_total
    degraded_total += 1
    _buckets.setdefault(int(time.time()), Counter())["degraded"] += 1
