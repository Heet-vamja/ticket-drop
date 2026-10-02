"""M1: naive booking with an in-memory dict. Deliberately racy: check and write are separated by an await."""
import asyncio

from fastapi import APIRouter

from .config import TOTAL_SEATS

router = APIRouter(prefix="/naive")

seats: dict[int, str | None] = {i: None for i in range(TOTAL_SEATS)}
sold_log: list[tuple[int, str]] = []


@router.post("/reset")
async def reset():
    for i in seats:
        seats[i] = None
    sold_log.clear()
    return {"ok": True}


@router.post("/book/{seat_id}")
async def book(seat_id: int, user: str):
    if seats.get(seat_id) is None:  # check
        await asyncio.sleep(0.005)  # stands in for a ~5ms DB round trip; lets other requests interleave
        seats[seat_id] = user  # write
        sold_log.append((seat_id, user))
        return {"status": "sold"}
    return {"status": "taken"}


@router.get("/stats")
async def stats():
    per_seat: dict[int, int] = {}
    for seat_id, _ in sold_log:
        per_seat[seat_id] = per_seat.get(seat_id, 0) + 1
    oversold = sum(c - 1 for c in per_seat.values() if c > 1)
    return {"sales": len(sold_log), "distinct_seats": len(per_seat), "oversold": oversold}
