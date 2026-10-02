"""M5 anti-bot: cheap rejection before any expensive work. No DB calls, only Redis counters."""
import hashlib
import secrets

from fastapi import HTTPException, Request

from . import metrics
from .config import ACCOUNT_BURST, ACCOUNT_RATE, IP_BURST, IP_RATE, POW_BITS, TRUST_XFF
from .holds import redis

# Token bucket, atomic in Redis: refill by elapsed time, take one token if available.
_BUCKET = redis.register_script(
    """
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local d = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(d[1]) or tonumber(ARGV[2])
local ts = tonumber(d[2]) or now
tokens = math.min(tonumber(ARGV[2]), tokens + (now - ts) * tonumber(ARGV[1]))
local ok = 0
if tokens >= 1 then tokens = tokens - 1; ok = 1 end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('EXPIRE', KEYS[1], 120)
return ok
"""
)


def client_ip(request: Request) -> str:
    if TRUST_XFF:
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


async def take(scope: str, ident: str, rate: float, burst: int) -> bool:
    return bool(await _BUCKET(keys=[f"tb:{scope}:{ident}"], args=[rate, burst]))


async def throttle(request: Request, account: str | None = None) -> None:
    """429 if the IP or the account is over budget. Counts throttles for the admin dashboard."""
    ip = client_ip(request)
    if not await take("ip", ip, IP_RATE, IP_BURST) or (
        account is not None and not await take("acct", account, ACCOUNT_RATE, ACCOUNT_BURST)
    ):
        await metrics.incr("throttled")
        raise HTTPException(429, "slow down")


# ---- proof of work: costs a browser ~0.1-1s once, costs a bot farm real CPU per account ----

def _leading_zero_bits(digest: bytes) -> int:
    n = int.from_bytes(digest, "big")
    return len(digest) * 8 - n.bit_length()


async def issue_challenge(user: str) -> dict:
    nonce = secrets.token_hex(8)
    await redis.set(f"pow:{user}", nonce, ex=120)
    return {"nonce": nonce, "bits": POW_BITS}


def solves(nonce: str, user: str, solution: str, bits: int) -> bool:
    digest = hashlib.sha256(f"{nonce}:{user}:{solution}".encode()).digest()
    return _leading_zero_bits(digest) >= bits


async def require_pow(user: str, solution: str | None) -> None:
    if POW_BITS == 0:
        return
    nonce = await redis.getdel(f"pow:{user}")  # single use
    if not nonce or not solution or not solves(nonce, user, solution, POW_BITS):
        await metrics.incr("rejected")
        raise HTTPException(400, "proof of work missing or wrong")


def solve(nonce: str, user: str, bits: int) -> str:
    """Reference solver (used by the simulator and tests; the browser has its own in TS)."""
    i = 0
    while not solves(nonce, user, str(i), bits):
        i += 1
    return str(i)
