"""Admin API: one real-time snapshot per second (SSE) plus controls. Every number comes from the live system:
Redis INFO, Postgres stats, per-second request buckets, the event-loop lag monitor, and the sim subprocess's counters.
"""
import asyncio
import json
import os
import signal
import sys
import time

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import func, select, text

from . import admission, booking, holds, metrics, queue
from .auth import require_admin
from .config import GATE, TOTAL_SEATS
from .db import Sale, Session, reset_sales

router = APIRouter(prefix="/admin", dependencies=[Depends(require_admin)])

SIM_STATUS = "sim:status"
SIM_PID = "sim:pid"
_chaos_until = 0.0
_cache: dict = {"at": 0.0, "data": None}
_lock = asyncio.Lock()
_pg_prev: dict = {"t": 0.0, "xacts": 0}


async def _redis_section() -> dict:
    r = holds.redis
    t0 = time.perf_counter()
    await r.ping()
    rtt = (time.perf_counter() - t0) * 1000
    pipe = r.pipeline()
    pipe.zcard(queue.QUEUE)
    pipe.zcount(admission.ADMITTED, time.time(), "+inf")
    pipe.hgetall(metrics.COUNTERS)
    pipe.lrange(metrics.EVENT_LOG, 0, 29)
    pipe.get(queue.OPENED_AT)
    pipe.hgetall(SIM_STATUS)
    pipe.mget([holds.seat_key(booking.EVENT_ID, i) for i in range(TOTAL_SEATS)])
    pipe.info()
    queued, active, counters, events, opened, sim, seat_vals, info = await pipe.execute()
    hits, misses = info.get("keyspace_hits", 0), info.get("keyspace_misses", 0)
    return {
        "queued": queued,
        "active": active,
        "counters": {k: int(v) for k, v in counters.items()},
        "events": [json.loads(e) for e in events],
        "opened_at": float(opened) if opened else None,
        "sim": sim,
        "seat_vals": seat_vals,
        "redis": {
            "up": True,
            "rtt_ms": round(rtt, 2),
            "ops_per_sec": info.get("instantaneous_ops_per_sec", 0),
            "clients": info.get("connected_clients", 0),
            "blocked": info.get("blocked_clients", 0),
            "memory_mb": round(info.get("used_memory", 0) / 1_048_576, 2),
            "total_commands": info.get("total_commands_processed", 0),
            "hit_ratio": round(hits / (hits + misses), 3) if hits + misses else None,
            "keys": sum(v.get("keys", 0) for k, v in info.items() if k.startswith("db") and isinstance(v, dict)),
        },
    }


async def _db_section() -> dict:
    async with Session() as s:
        total, distinct = (await s.execute(select(func.count(), func.count(func.distinct(Sale.seat_id))))).one()
        sold = (await s.scalars(select(Sale.seat_id).where(Sale.event_id == booking.EVENT_ID))).all()
        backends, xacts, active = (
            await s.execute(
                text(
                    "SELECT d.numbackends, d.xact_commit + d.xact_rollback, "
                    "(SELECT count(*) FROM pg_stat_activity WHERE datname = d.datname AND state = 'active') "
                    "FROM pg_stat_database d WHERE d.datname = current_database()"
                )
            )
        ).one()
    now = time.time()
    tps = 0.0
    if _pg_prev["t"]:
        tps = max(0.0, (xacts - _pg_prev["xacts"]) / max(now - _pg_prev["t"], 0.001))
    _pg_prev.update(t=now, xacts=xacts)
    return {
        "sold": set(sold),
        "pg": {
            "up": True,
            "connections": backends,
            "active": active,
            "tps": round(tps, 1),
            "sales": total,
            "oversold": total - distinct,
        },
    }


async def build_snapshot() -> dict:
    snap: dict = {"ts": time.time(), "chaos_until": _chaos_until if _chaos_until > time.time() else None}
    seat_vals: list = [None] * TOTAL_SEATS
    try:
        r = await asyncio.wait_for(_redis_section(), 2.5)
        seat_vals = r.pop("seat_vals")
        snap.update(r)
    except Exception as e:  # Redis paused/down: say so instead of going silent
        snap["redis"] = {"up": False, "error": type(e).__name__}
        snap.update(queued=None, active=None, counters={}, events=[], opened_at=None, sim={})
    sold: set[int] = set()
    try:
        d = await asyncio.wait_for(_db_section(), 2.5)
        sold = d["sold"]
        snap["pg"] = d["pg"]
    except Exception as e:
        snap["pg"] = {"up": False, "error": type(e).__name__}
    states = "".join("s" if i in sold else ("h" if v is not None else "a") for i, v in enumerate(seat_vals))
    snap["seats"] = {
        "states": states,
        "sold": states.count("s"),
        "held": states.count("h"),
        "available": states.count("a"),
        "total": TOTAL_SEATS,
    }
    snap["rps"] = metrics.rps_series(60)
    snap["latency"] = metrics.latency_stats(10)
    snap["api"] = {"loop_lag_ms": round(metrics.loop_lag_ms, 1), "pid": os.getpid()}
    snap["gate"] = {**GATE, "measured_rate": round(admission.stats["rate"], 1), "ewma_ms": round(metrics.ewma_ms, 1), "target_ms": 150}
    return snap


async def get_snapshot() -> dict:
    async with _lock:  # all admin tabs share one computation per second
        if time.time() - _cache["at"] > 0.9:
            _cache["data"] = await build_snapshot()
            _cache["at"] = time.time()
        return _cache["data"]


@router.get("/stream")
async def stream():
    async def gen():
        while True:
            yield f"data: {json.dumps(await get_snapshot())}\n\n"
            await asyncio.sleep(1)

    return StreamingResponse(gen(), media_type="text/event-stream")


# ---------------- controls ----------------

async def _sim_running() -> int | None:
    pid = await holds.redis.get(SIM_PID)
    if not pid:
        return None
    try:
        os.killpg(int(pid), 0)  # the sim runs in its own process group (parent + shards + bots)
        return int(pid)
    except (ProcessLookupError, ValueError):
        return None


@router.post("/drop/open")
async def open_drop():
    return await queue.open_queue()


async def _wait_quiet(max_seconds: float = 10.0) -> None:
    """Block until the API has served no booking/queue traffic for ~1.5s, so in-flight requests can't land
    after the wipe (they'd re-create sales/holds)."""
    quiet_since = time.time()
    deadline = time.time() + max_seconds
    while time.time() < deadline:
        now = int(time.time())
        busy = any(
            metrics._buckets.get(sec, {}).get(g, 0)
            for sec in (now, now - 1)
            for g in ("booking", "queue")
        )
        if busy:
            quiet_since = time.time()
        elif time.time() - quiet_since >= 1.5:
            return
        await asyncio.sleep(0.25)


async def _wipe() -> None:
    await stop_sim()
    await _wait_quiet()
    await reset_sales()
    r = holds.redis
    patterns = ["seat:*", "admit:*", "user:*", "tb:*", "pow:*"]
    for pattern in patterns:
        keys = [k async for k in r.scan_iter(pattern, count=1000)]
        if keys:
            await r.delete(*keys)
    await r.delete(queue.QUEUE, admission.ADMITTED, metrics.COUNTERS, metrics.EVENT_LOG, booking.SOLD_SET,
                   holds.DEADLINES, queue.OPENED_AT, SIM_STATUS)
    _cache["at"] = 0


@router.post("/reset")
async def reset():
    await _wipe()
    return {"ok": True}


class SimRequest(BaseModel):
    humans: int = 1000
    bots: int = 200


@router.post("/simulate")
async def simulate(req: SimRequest):
    if await _sim_running():
        return {"ok": False, "error": "a simulation is already running"}
    if await holds.redis.get(queue.OPENED_AT) is None:
        await queue.open_queue()
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "app.sim", str(req.humans), str(req.bots),
        stdout=open("sim.log", "ab"), stderr=asyncio.subprocess.STDOUT, start_new_session=True,
    )
    await holds.redis.set(SIM_PID, proc.pid)
    return {"ok": True, "pid": proc.pid}


@router.post("/simulate/stop")
async def stop_sim():
    pid = await _sim_running()
    if pid:
        os.killpg(pid, signal.SIGTERM)
        for _ in range(30):  # wait for the whole group to exit so nothing keeps buying after a reset
            await asyncio.sleep(0.1)
            if not await _sim_running():
                break
        else:
            os.killpg(pid, signal.SIGKILL)
    await holds.redis.delete(SIM_PID)
    return {"ok": True, "stopped": bool(pid)}


class GateRequest(BaseModel):
    batch: int | None = None
    max_active: int | None = None
    adaptive: bool | None = None


@router.post("/gate")
async def set_gate(req: GateRequest):
    for field, value in req.model_dump(exclude_none=True).items():
        GATE[field] = value
    return GATE


class ChaosRequest(BaseModel):
    seconds: int = 5


@router.post("/chaos/redis-pause")
async def redis_pause(req: ChaosRequest):
    """Real fault injection: Redis stops answering every client (CLIENT PAUSE ALL) for N seconds."""
    global _chaos_until
    seconds = max(1, min(30, req.seconds))
    _chaos_until = time.time() + seconds
    await metrics.log_event("chaos", f"Redis paused for {seconds}s")  # log first: Redis won't answer afterwards
    await holds.redis.client_pause(seconds * 1000, all=True)
    return {"paused_for": seconds}
