# Failure modes (observed, not theoretical)

Every row below was reproduced against the running system using the admin console ("Freeze Redis" = `CLIENT PAUSE ALL`).

| Failure | What the system does | Evidence |
|---|---|---|
| Redis unresponsive (8s freeze, ~2,500 simulated users) | **Fails closed.** Redis client times out in 1s; a global handler returns `503` + `Retry-After: 2`. No holds, no queue joins, admission paused. The admin stream keeps working and shows `REDIS DOWN`. | 1,324 fast 503s in 4s; Postgres `oversold` stayed `0`; selling resumed on recovery with no manual action |
| Redis loses data (restart without persistence) | Holds and queue are gone. Sold seats are safe: Postgres is the source of truth and `UNIQUE(event_id, seat_id)` rejects any second sale; a stale confirm gets `409`. Users must rejoin the queue. | Unique-constraint test in `tests/test_m2.py` |
| Hold expires while the user is paying | `confirm` is idempotent and re-checks ownership. If the seat is still free it grants a 30s grace extension; if someone else took it, the user gets `410` (refund). | `booking.confirm`, `extend_hold` |
| Admitted user goes idle | Admission JWT expires after 90s; the gate frees the slot (`ZREMRANGEBYSCORE`) and admits the next person. Buying also frees the slot immediately. | `tests/test_m4.py::test_max_active_caps_admission_then_expiry_frees_slots` |
| Booking latency rises | Gate halves its batch when the booking-latency EWMA exceeds 150ms and grows back by 2/s once it is under 90ms. | `tests/test_m4.py::test_adaptive_gate_shrinks_under_latency_and_recovers`; visible live in the gate panel |
| Show sells out | Joins return `410` immediately, queued users get a `soldout` SSE event, admission stops. No one waits for seats that don't exist. | `queue.join`, `queue.position_of` |
| Bots | Per-IP and per-account token buckets (Lua, atomic) answer `429` with no DB work; proof-of-work (14 bits) at queue entry; max 4 tickets per account. | 28k bot requests blocked, 0 got through, in a 3,000-human / 400-bot run |

## Known limits (honest list)

- One uvicorn worker. Metrics buckets and the latency EWMA are in-process; scale-out needs them in Redis.
- Identity is a self-chosen name. There is no login, so "one account = one queue slot" is only as strong as the account system you put in front of it. `X-Forwarded-For` is trusted (`TRUST_XFF=1`) for the simulator; behind a real proxy, trust only its header.
- Redis is single-node. A real deployment needs Sentinel/Cluster, and the failure behaviour above should be re-tested there.
- The simulator and the API share one laptop, so latency numbers at 10k users include the load generator's own CPU use.
