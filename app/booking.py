import time

from fastapi import APIRouter, HTTPException
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from . import holds
from .config import TOTAL_SEATS
from .db import Sale, Session, reset_sales

router = APIRouter(prefix="/v2")
EVENT_ID = 1
GRACE_SECONDS = 30
SOLD_SET = "sold:seats"


@router.post("/reset")
async def reset():
    await reset_sales()
    keys = [k async for k in holds.redis.scan_iter("seat:*")]
    if keys:
        await holds.redis.delete(*keys)
    await holds.redis.delete(holds.DEADLINES, SOLD_SET)
    _seat_cache["at"] = 0.0
    return {"ok": True}


@router.post("/hold/{seat_id}")
async def hold(seat_id: int, user: str):
    if not 0 <= seat_id < TOTAL_SEATS:
        raise HTTPException(404, "no such seat")
    if not await holds.try_hold(EVENT_ID, seat_id, user):
        raise HTTPException(409, "seat taken")
    return {"status": "held"}


@router.post("/confirm/{seat_id}")
async def confirm(seat_id: int, user: str):
    """Idempotent: re-confirming your own sale succeeds; losing the hold to someone else means refund."""
    if not await holds.owns_hold(EVENT_ID, seat_id, user):
        # Hold expired between pay and confirm: try a short grace extension if the seat is still free.
        if not await holds.extend_hold(EVENT_ID, seat_id, user, GRACE_SECONDS):
            async with Session() as s:
                sale = await s.scalar(
                    select(Sale).where(Sale.event_id == EVENT_ID, Sale.seat_id == seat_id)
                )
            if sale and sale.user_id == user:
                return {"status": "sold"}  # retry of an already-completed confirm
            raise HTTPException(410, "hold lost; payment will be refunded")
    async with Session() as s:
        s.add(Sale(event_id=EVENT_ID, seat_id=seat_id, user_id=user))
        try:
            await s.commit()
        except IntegrityError:
            await s.rollback()
            existing = await s.scalar(
                select(Sale).where(Sale.event_id == EVENT_ID, Sale.seat_id == seat_id)
            )
            if existing and existing.user_id == user:
                return {"status": "sold"}
            raise HTTPException(409, "seat already sold")
    # SOLD is terminal: keep the key so nobody can re-hold, but drop the expiry.
    await holds.redis.sadd(SOLD_SET, seat_id)
    await holds.redis.persist(holds.seat_key(EVENT_ID, seat_id))
    await holds.redis.zrem(holds.DEADLINES, f"{EVENT_ID}:{seat_id}")
    return {"status": "sold"}


@router.post("/book/{seat_id}")
async def book(seat_id: int, user: str):
    """hold -> (simulated payment) -> confirm in one call, same shape as /naive/book for the race script."""
    try:
        await hold(seat_id, user)
    except HTTPException:
        return {"status": "taken"}
    try:
        return await confirm(seat_id, user)
    except HTTPException:
        return {"status": "taken"}


@router.get("/stats")
async def stats():
    async with Session() as s:
        total = await s.scalar(select(func.count()).select_from(Sale))
        distinct = await s.scalar(select(func.count(func.distinct(Sale.seat_id))))
    return {"sales": total, "distinct_seats": distinct, "oversold": total - distinct}


_seat_cache: dict = {"at": 0.0, "value": ""}
SEAT_CACHE_SECONDS = 1.0


async def seat_states() -> str:
    """One char per seat: 'a' available, 'h' held, 's' sold. Sold seats keep a persistent key (see confirm).
    Cached for 1s: thousands of shoppers refreshing the map shouldn't each cost an MGET + a DB query. Holds stay
    atomic, so a stale map can only cause a harmless 409 'seat taken'."""
    if time.time() - _seat_cache["at"] < SEAT_CACHE_SECONDS and _seat_cache["value"]:
        return _seat_cache["value"]
    value = await _compute_seat_states()
    _seat_cache.update(at=time.time(), value=value)
    return value


async def _compute_seat_states() -> str:
    keys = [holds.seat_key(EVENT_ID, i) for i in range(TOTAL_SEATS)]
    values = await holds.redis.mget(keys)
    async with Session() as s:
        sold = set((await s.scalars(select(Sale.seat_id).where(Sale.event_id == EVENT_ID))).all())
    return "".join("s" if i in sold else ("h" if v is not None else "a") for i, v in enumerate(values))


async def sold_out() -> bool:
    return await holds.redis.scard(SOLD_SET) >= TOTAL_SEATS
