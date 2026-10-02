"""Token-guarded booking API used by the React app. Identity comes from the admission JWT, never the query."""
import jwt
from fastapi import APIRouter, Header, HTTPException, Request

from . import admission, antibot, booking, holds, metrics
from .config import MAX_TICKETS_PER_USER

router = APIRouter(prefix="/shop")


def _user_from(authorization: str | None, verify_exp: bool) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "missing admission token")
    try:
        return admission.verify_token(authorization[7:], verify_exp=verify_exp)
    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "admission expired; rejoin the queue")
    except jwt.InvalidTokenError:
        raise HTTPException(401, "invalid admission token")


def _tickets_key(user: str) -> str:
    return f"user:{user}:seats"


async def tickets_in_play(user: str) -> int:
    """Seats this user currently holds or owns. Stale entries (expired holds) are pruned lazily."""
    seats = await holds.redis.smembers(_tickets_key(user))
    live = 0
    for seat in seats:
        if await holds.redis.get(holds.seat_key(booking.EVENT_ID, int(seat))) == user:
            live += 1
        else:
            await holds.redis.srem(_tickets_key(user), seat)
    return live


@router.get("/seats")
async def seats(request: Request, authorization: str | None = Header(None)):
    user = _user_from(authorization, verify_exp=False)
    await antibot.throttle(request, account=user)
    return {"seats": await booking.seat_states()}


@router.post("/hold/{seat_id}")
async def hold(request: Request, seat_id: int, authorization: str | None = Header(None)):
    user = _user_from(authorization, verify_exp=True)
    await antibot.throttle(request, account=user)
    if await tickets_in_play(user) >= MAX_TICKETS_PER_USER:
        await metrics.incr("capped")
        raise HTTPException(403, f"max {MAX_TICKETS_PER_USER} tickets per person")
    await booking.hold(seat_id, user)  # 409 if taken
    await holds.redis.sadd(_tickets_key(user), seat_id)
    await metrics.incr("held")
    await metrics.log_event("hold", f"{user} holds seat #{seat_id}")
    return {"status": "held", "ttl": holds.HOLD_TTL_SECONDS}


@router.post("/confirm/{seat_id}")
async def confirm(request: Request, seat_id: int, authorization: str | None = Header(None)):
    user = _user_from(authorization, verify_exp=False)
    await antibot.throttle(request, account=user)  # paying after the admission window is fine if you hold the seat
    result = await booking.confirm(seat_id, user)
    await metrics.incr("sold")
    await metrics.log_event("sold", f"{user} booked seat #{seat_id}")
    return result


@router.post("/release/{seat_id}")
async def release(seat_id: int, authorization: str | None = Header(None)):
    user = _user_from(authorization, verify_exp=False)
    await holds.release(booking.EVENT_ID, seat_id, user)
    await holds.redis.srem(_tickets_key(user), seat_id)
    return {"status": "released"}
