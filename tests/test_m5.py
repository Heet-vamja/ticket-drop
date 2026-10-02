import asyncio

import pytest
from fastapi import HTTPException

from app import antibot, queue, shop
from app.config import POW_BITS


class FakeReq:
    def __init__(self, ip):
        self.headers = {"x-forwarded-for": ip}
        self.client = None


@pytest.fixture(autouse=True)
async def fresh():
    keys = [k async for k in antibot.redis.scan_iter("tb:*")]
    if keys:
        await antibot.redis.delete(*keys)


async def test_bucket_allows_burst_then_throttles():
    results = await asyncio.gather(*(antibot.take("t", "x", 1.0, 5) for _ in range(50)))
    assert sum(results) == 5  # burst size, atomic even under concurrency


async def test_bot_hammering_is_throttled_but_other_ips_unaffected():
    bot = FakeReq("6.6.6.6")
    denied = 0
    for _ in range(300):
        try:
            await antibot.throttle(bot, account="botacct")
        except HTTPException as e:
            assert e.status_code == 429
            denied += 1
    assert denied > 250
    await antibot.throttle(FakeReq("1.2.3.4"), account="human")  # unaffected


async def test_pow_roundtrip_and_single_use():
    ch = await antibot.issue_challenge("alice")
    sol = antibot.solve(ch["nonce"], "alice", ch["bits"])
    await antibot.require_pow("alice", sol)
    with pytest.raises(HTTPException):  # replay of the same solution
        await antibot.require_pow("alice", sol)


async def test_pow_wrong_solution_rejected():
    await antibot.issue_challenge("mallory")
    if POW_BITS:
        with pytest.raises(HTTPException):
            await antibot.require_pow("mallory", "not-a-solution")


async def test_ticket_cap_enforced():
    from app import booking
    await booking.reset()
    user = "scalper"
    for seat in range(4):
        await booking.hold(seat, user)
        await antibot.redis.sadd(shop._tickets_key(user), seat)
    assert await shop.tickets_in_play(user) == 4
    # an expired hold stops counting
    await antibot.redis.delete("seat:1:0")
    assert await shop.tickets_in_play(user) == 3
