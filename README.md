# ticket-drop

500 seats, thousands of people clicking at once. Waiting room, admission gate, atomic holds, and a live control room.

```
Browser ─► [throttle + proof-of-work] ─► [waiting room (Redis ZSET)] ─► [admission gate (JWT)] ─► [holds (Redis SET NX EX)] ─► [Postgres UNIQUE(event, seat)]
```

## Run it

```bash
docker compose up -d redis postgres
uv venv --python 3.12 && uv pip install -e ".[dev]"
.venv/bin/uvicorn app.main:app --port 8765
cd frontend && npm install && npm run dev        # http://localhost:5173
```

- Shop (waiting room + seat map): `http://localhost:5173/#/`
- Control room: `http://localhost:5173/#/admin` (key defaults to `dev-admin`; set `ADMIN_KEY`)
- In the control room: **Open the drop**, then **Run simulation** (real HTTP traffic from separate processes), or join yourself from the shop in another tab.

## Milestones

| | What | Where |
|---|---|---|
| M1 | Naive in-memory booking that oversells | `app/naive.py`, `loadtest/race.py` |
| M2 | Atomic Redis holds, Postgres unique constraint, idempotent confirm, expiry sweeper | `app/holds.py`, `app/booking.py`, `app/expiry.py`, `app/db.py` |
| M3 | Waiting room: shuffled entry window, SSE position/ETA | `app/queue.py`, `frontend/src/Shop.tsx` |
| M4 | Admission gate: signed JWTs, adaptive batch, slot re-use | `app/admission.py`, `app/shop.py` |
| M5 | Token buckets (IP + account), proof-of-work, 4-ticket cap | `app/antibot.py` |
| M6 | Telemetry, chaos (freeze Redis), failure modes | `app/metrics.py`, `app/admin.py`, `docs/failure-modes.md` |

Race demo: `python loadtest/race.py /naive 10000 20` oversells; `python loadtest/race.py /v2 10000 20` does not.

Tests: `.venv/bin/pytest` (needs Redis and Postgres up).
