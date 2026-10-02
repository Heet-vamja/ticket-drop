"""Load simulator: real HTTP traffic against the running API, from separate processes.

    python -m app.sim <humans> <bots>

Humans are sharded across several processes (one Python event loop tops out around ~1-2k req/s). Each fetches a
proof-of-work challenge, solves it, joins the queue, polls its position with jitter, gets admitted, grabs a seat,
"pays" and confirms. Bots live in their own process: no proof of work, a few IPs, hammering the join endpoint.
Every process reports deltas into the Redis hash sim:status, which the admin dashboard reads.
"""
import asyncio
import math
import multiprocessing
import os
import random
import signal
import sys
import time
from concurrent.futures import ProcessPoolExecutor

import httpx
import redis.asyncio as aioredis

from .antibot import solve

BASE = os.environ.get("BASE", "http://127.0.0.1:8765")
REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")
STATUS = "sim:status"
HUMANS_PER_PROCESS = 1000

stop = asyncio.Event()


class Stats:
    """Local counters, flushed to Redis as deltas so many processes can share one hash."""

    def __init__(self) -> None:
        self.v: dict[str, int] = {}
        self._sent: dict[str, int] = {}

    def inc(self, key: str, by: int = 1) -> None:
        self.v[key] = self.v.get(key, 0) + by

    async def flush_forever(self, r: aioredis.Redis) -> None:
        while True:
            await self.flush(r)
            await asyncio.sleep(1)

    async def flush(self, r: aioredis.Redis) -> None:
        pipe = r.pipeline()
        for k, v in self.v.items():
            if v != self._sent.get(k, 0):
                pipe.hincrby(STATUS, k, v - self._sent.get(k, 0))
                self._sent[k] = v
        await pipe.execute()


def _ip(i: int) -> str:
    return f"10.{(i >> 16) & 255}.{(i >> 8) & 255}.{i & 255}"


async def send(client: httpx.AsyncClient, method: str, url: str, **kw) -> httpx.Response:
    """Retry transient transport errors (e.g. the server closing an idle keep-alive connection). Every call a human
    makes is safe to repeat: challenge/position are reads, join is NX, hold is NX, confirm is idempotent."""
    for attempt in range(3):
        try:
            return await client.request(method, url, **kw)
        except httpx.TransportError:
            if attempt == 2:
                raise
            await asyncio.sleep(0.2 * (attempt + 1))
    raise AssertionError("unreachable")


async def human(i: int, client: httpx.AsyncClient, pool: ProcessPoolExecutor, spread: float, st: Stats) -> None:
    user, headers = f"h{i}", {"X-Forwarded-For": _ip(i + 1)}
    loop = asyncio.get_running_loop()
    try:
        await asyncio.sleep(random.random() * spread)  # everybody clicks "join" within the same few seconds
        ch = (await send(client, "GET", "/queue/challenge", params={"user": user}, headers=headers)).json()
        solution = await loop.run_in_executor(pool, solve, ch["nonce"], user, ch["bits"])
        st.inc("pow_solved")
        r = await send(client, "POST", "/queue/join", params={"user": user, "solution": solution}, headers=headers)
        if r.status_code == 410:
            st.inc("sold_out")
            return
        r.raise_for_status()
        st.inc("joined")
        token = None
        wait = r.json().get("poll_after", 3.0)
        while not stop.is_set():
            try:  # obey the server's backpressure hint, with jitter; wake immediately on stop
                await asyncio.wait_for(stop.wait(), timeout=wait * (0.8 + 0.4 * random.random()))
                return
            except asyncio.TimeoutError:
                pass
            r = await send(client, "GET", "/queue/position", params={"user": user}, headers=headers)
            if r.status_code == 404:
                st.inc("gave_up")
                return
            if r.status_code == 410:
                st.inc("sold_out")
                return
            if r.status_code == 200 and r.json()["status"] == "admitted":
                token = r.json()["token"]
                break
            if r.status_code == 200:
                wait = r.json().get("poll_after", wait)
        if not token:
            return
        st.inc("admitted")
        auth = {**headers, "Authorization": f"Bearer {token}"}
        for _ in range(8):  # pick a seat; popular front seats are contended
            r = await send(client, "GET", "/shop/seats", headers=auth)
            if r.status_code != 200:
                st.inc("errors")
                return
            free = [n for n, c in enumerate(r.json()["seats"]) if c == "a"]
            if not free:
                st.inc("sold_out")
                return
            seat = random.choice(free[: max(5, len(free) // 3)])  # bias toward the front third
            h = await send(client, "POST", f"/shop/hold/{seat}", headers=auth)
            if h.status_code == 409:
                continue
            if h.status_code != 200:
                st.inc("errors")
                return
            await asyncio.sleep(1 + random.random() * 4)  # entering payment details
            c = await send(client, "POST", f"/shop/confirm/{seat}", headers=auth)
            st.inc("bought" if c.status_code == 200 else "errors")
            return
        st.inc("gave_up")
    except Exception as e:
        st.inc("errors")
        if st.v["errors"] == 1:
            print(f"first human error: {e!r}", flush=True)


async def _humans_main(first: int, count: int, spread: float) -> None:
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stop.set)
    st, r = Stats(), aioredis.from_url(REDIS_URL, decode_responses=True)
    flusher = asyncio.create_task(st.flush_forever(r))
    limits = httpx.Limits(max_connections=800, max_keepalive_connections=800)
    with ProcessPoolExecutor(max_workers=2) as pool:
        async with httpx.AsyncClient(base_url=BASE, limits=limits, timeout=30) as client:
            await asyncio.gather(*(human(first + i, client, pool, spread, st) for i in range(count)))
    flusher.cancel()
    await st.flush(r)
    await r.aclose()


def humans_process(first: int, count: int, spread: float) -> None:
    asyncio.run(_humans_main(first, count, spread))


async def bot(i: int, client: httpx.AsyncClient, st: Stats) -> None:
    headers = {"X-Forwarded-For": f"203.0.113.{i % 4}"}  # a bot farm: many accounts, few IPs
    while not stop.is_set():
        try:
            r = await client.post("/queue/join", params={"user": f"bot{i}"}, headers=headers)  # no proof of work
            st.inc("bot_reqs")
            st.inc({429: "bot_throttled", 400: "bot_rejected"}.get(r.status_code, "bot_other"))
        except Exception:
            pass
        await asyncio.sleep(0.15 + random.random() * 0.3)


async def _bots_main(bots: int) -> None:
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stop.set)
    st, r = Stats(), aioredis.from_url(REDIS_URL, decode_responses=True)
    flusher = asyncio.create_task(st.flush_forever(r))
    async with httpx.AsyncClient(base_url=BASE, limits=httpx.Limits(max_connections=400), timeout=30) as client:
        await asyncio.gather(*(bot(i, client, st) for i in range(bots)))
    flusher.cancel()
    await r.aclose()


def bots_process(bots: int) -> None:
    asyncio.run(_bots_main(bots))


async def main(humans: int, bots: int) -> None:
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop.set)
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    await r.delete(STATUS)
    await r.hset(STATUS, mapping={"running": 1, "humans_total": humans, "bots_total": bots, "started_at": time.time()})

    shards = max(1, math.ceil(humans / HUMANS_PER_PROCESS))
    per = math.ceil(humans / shards)
    spread = min(30.0, max(3.0, humans / 300))
    procs = [
        multiprocessing.Process(target=humans_process, args=(s * per, min(per, humans - s * per), spread))  # non-daemon: each shard runs a PoW process pool
        for s in range(shards)
    ]
    bot_proc = multiprocessing.Process(target=bots_process, args=(bots,), daemon=True) if bots else None
    for p in procs + ([bot_proc] if bot_proc else []):
        p.start()

    while any(p.is_alive() for p in procs) and not stop.is_set():
        await asyncio.sleep(0.5)
    for p in procs + ([bot_proc] if bot_proc else []):
        if p.is_alive():
            p.terminate()
    await asyncio.sleep(1.5)  # let the children flush their last deltas
    await r.hset(STATUS, "running", 0)
    await r.delete("sim:pid")
    await r.aclose()


if __name__ == "__main__":
    asyncio.run(main(int(sys.argv[1]), int(sys.argv[2])))
