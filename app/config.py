import os

TOTAL_SEATS = 500
HOLD_TTL_SECONDS = 300
MAX_TICKETS_PER_USER = 4

# Waiting room / admission
ENTRY_WINDOW_SECONDS = int(os.environ.get("ENTRY_WINDOW_SECONDS", "60"))
ADMIT_INTERVAL_SECONDS = float(os.environ.get("ADMIT_INTERVAL_SECONDS", "1"))
ADMISSION_TTL_SECONDS = 90  # time an admitted user has to start a hold before the slot is re-used
LATENCY_TARGET_MS = 150  # adaptive gate shrinks the batch above this booking latency
MIN_BATCH, MAX_BATCH = 2, 100

# Live, admin-adjustable gate state. batch = users admitted per tick; max_active = admitted-but-not-done cap
# (what the booking service can handle at once).
GATE = {"batch": 20, "max_active": 100, "adaptive": True}

JWT_SECRET = os.environ.get("JWT_SECRET", "dev-secret-change-me-this-is-32-bytes-or-more")
ADMIN_KEY = os.environ.get("ADMIN_KEY", "dev-admin")

# Anti-bot
POW_BITS = int(os.environ.get("POW_BITS", "14"))  # 0 disables proof-of-work
IP_RATE, IP_BURST = 30.0, 60  # tokens/sec, bucket size (shared NATs exist, so generous)
ACCOUNT_RATE, ACCOUNT_BURST = 3.0, 8
TRUST_XFF = os.environ.get("TRUST_XFF", "1") == "1"  # dev/sim only; behind a real proxy, trust only its header
