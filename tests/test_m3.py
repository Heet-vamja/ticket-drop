import asyncio

import pytest

from app import queue


@pytest.fixture(autouse=True)
async def opened():
    await queue.open_queue()


async def test_cannot_join_before_open():
    await queue.redis.delete(queue.OPENED_AT)
    with pytest.raises(Exception):
        await queue.join("early")


async def test_rejoin_keeps_position():
    first = await queue.join("alice")
    again = await queue.join("alice")
    assert first["position"] == again["position"]
    assert again["total"] == 1


async def test_window_positions_are_shuffled_not_fifo():
    users = [f"u{i}" for i in range(200)]
    for u in users:
        await queue.join(u)
    order = [m for m in await queue.redis.zrange(queue.QUEUE, 0, -1)]
    assert order != users  # 1/200! chance of a false failure


async def test_after_window_is_fifo_behind_lottery(monkeypatch):
    await queue.join("early")
    monkeypatch.setattr(queue, "ENTRY_WINDOW_SECONDS", -1)  # window already closed
    await queue.join("late1")
    await asyncio.sleep(0.01)
    await queue.join("late2")
    order = await queue.redis.zrange(queue.QUEUE, 0, -1)
    assert order == ["early", "late1", "late2"]
