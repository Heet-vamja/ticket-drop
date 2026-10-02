import asyncio

import pytest
from sqlalchemy.exc import IntegrityError

from app import booking, holds
from app.db import Sale, Session, init_db, reset_sales


@pytest.fixture(autouse=True)
async def clean():
    await init_db()
    await booking.reset()
    yield


async def test_only_one_of_many_gets_the_hold():
    results = await asyncio.gather(*(holds.try_hold(1, 7, f"u{i}") for i in range(200)))
    assert sum(results) == 1


async def test_release_only_by_owner():
    await holds.try_hold(1, 3, "alice")
    assert not await holds.release(1, 3, "bob")
    assert await holds.release(1, 3, "alice")
    assert await holds.try_hold(1, 3, "bob")


async def test_db_constraint_blocks_double_sale():
    async with Session() as s:
        s.add(Sale(event_id=1, seat_id=1, user_id="a"))
        await s.commit()
    async with Session() as s:
        s.add(Sale(event_id=1, seat_id=1, user_id="b"))
        with pytest.raises(IntegrityError):
            await s.commit()


async def test_confirm_is_idempotent():
    await booking.hold(5, "alice")
    assert (await booking.confirm(5, "alice"))["status"] == "sold"
    assert (await booking.confirm(5, "alice"))["status"] == "sold"
    assert (await booking.stats())["sales"] == 1
