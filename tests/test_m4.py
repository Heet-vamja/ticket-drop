import time

import jwt
import pytest

from app import admission, queue
from app.config import GATE, JWT_SECRET


@pytest.fixture(autouse=True)
async def fresh():
    await queue.open_queue()
    await admission.redis.delete(admission.ADMITTED)
    saved = dict(GATE)
    GATE.update(batch=3, max_active=5, adaptive=False)
    yield
    GATE.update(saved)


async def test_admits_batch_and_issues_valid_token():
    for u in ["a", "b", "c", "d"]:
        await queue.join(u)
    admitted = await admission.admit_once()
    assert len(admitted) == 3
    token = await admission.redis.get(admission.admit_key(admitted[0]))
    assert admission.verify_token(token) == admitted[0]
    assert (await queue.position_of(admitted[0]))["status"] == "admitted"


async def test_max_active_caps_admission_then_expiry_frees_slots():
    for i in range(12):
        await queue.join(f"u{i}")
    await admission.admit_once()  # 3
    await admission.admit_once()  # 2 more (cap 5)
    assert await admission.admit_once() == []  # full
    # expire everyone's slot -> next batch gets in
    await admission.redis.zadd(admission.ADMITTED, {u: time.time() - 1 for u in await admission.redis.zrange(admission.ADMITTED, 0, -1)})
    assert len(await admission.admit_once()) == 3


async def test_forged_and_expired_tokens_rejected():
    forged = jwt.encode({"sub": "x", "exp": time.time() + 60}, "wrong-secret", algorithm="HS256")
    with pytest.raises(jwt.InvalidTokenError):
        admission.verify_token(forged)
    expired = jwt.encode({"sub": "x", "exp": time.time() - 5}, JWT_SECRET, algorithm="HS256")
    with pytest.raises(jwt.ExpiredSignatureError):
        admission.verify_token(expired)
    assert admission.verify_token(expired, verify_exp=False) == "x"


async def test_adaptive_gate_shrinks_under_latency_and_recovers(monkeypatch):
    GATE.update(batch=40, adaptive=True)
    monkeypatch.setattr(admission.metrics, "ewma_ms", 500.0)
    admission.adapt()
    assert GATE["batch"] == 20
    monkeypatch.setattr(admission.metrics, "ewma_ms", 10.0)
    admission.adapt()
    assert GATE["batch"] == 22
