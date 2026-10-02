"""Fire N concurrent booking requests at a prefix (/naive, ...) and print the oversell stats.

Usage: python loadtest/race.py [prefix] [users] [hot_seats]
"""
import asyncio
import random
import sys

import httpx

import os
BASE = os.environ.get("BASE", "http://127.0.0.1:8765")


async def main(prefix: str, users: int, seats: int):
    limits = httpx.Limits(max_connections=500)
    async with httpx.AsyncClient(base_url=BASE, limits=limits, timeout=60) as c:
        await c.post(f"{prefix}/reset")
        sem = asyncio.Semaphore(500)

        async def one(i: int):
            async with sem:
                await c.post(f"{prefix}/book/{random.randrange(seats)}", params={"user": f"u{i}"})

        await asyncio.gather(*(one(i) for i in range(users)))
        print((await c.get(f"{prefix}/stats")).json())


if __name__ == "__main__":
    prefix = sys.argv[1] if len(sys.argv) > 1 else "/naive"
    users = int(sys.argv[2]) if len(sys.argv) > 2 else 10_000
    seats = int(sys.argv[3]) if len(sys.argv) > 3 else 20
    asyncio.run(main(prefix, users, seats))
