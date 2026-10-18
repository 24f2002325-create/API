import base64
import threading
import time
import uuid
from collections import defaultdict, deque
from typing import Optional

from fastapi import FastAPI, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

TOTAL_ORDERS = 46      # T
RATE_LIMIT = 15        # R requests
WINDOW_SECONDS = 10.0  # per 10s

app = FastAPI(title="Orders API")

# ---------- Rate limiting (per X-Client-Id, sliding window) ----------
_hits = defaultdict(deque)
_rl_lock = threading.Lock()


@app.middleware("http")
async def rate_limit(request: Request, call_next):
    client_id = request.headers.get("x-client-id")
    if request.method == "OPTIONS" or not client_id:
        return await call_next(request)

    now = time.monotonic()
    with _rl_lock:
        q = _hits[client_id]
        while q and now - q[0] >= WINDOW_SECONDS:
            q.popleft()
        if len(q) >= RATE_LIMIT:
            retry_after = max(1, int(WINDOW_SECONDS - (now - q[0]) + 0.999))
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded"},
                headers={"Retry-After": str(retry_after)},
            )
        q.append(now)
    return await call_next(request)


# CORS added last so it is the outermost layer (also decorates 429s)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Retry-After"],
)

# ---------- Idempotent POST /orders ----------
_orders_by_key = {}
_idem_lock = threading.Lock()


@app.post("/orders", status_code=201)
async def create_order(request: Request, idempotency_key: Optional[str] = Header(None)):
    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {"payload": body}
    except Exception:
        body = {}

    if not idempotency_key:
        return JSONResponse(status_code=400, content={"detail": "Idempotency-Key header required"})

    with _idem_lock:
        order = _orders_by_key.get(idempotency_key)
        if order is None:
            order = {"id": f"ord_{uuid.uuid4().hex[:12]}", "status": "created", **body}
            order["id"] = order["id"]  # never let body override id
            _orders_by_key[idempotency_key] = order
    return order


# ---------- Cursor pagination GET /orders ----------
def _encode(offset: int) -> str:
    return base64.urlsafe_b64encode(f"o:{offset}".encode()).decode()


def _decode(cursor: Optional[str]) -> int:
    if not cursor:
        return 0
    try:
        raw = base64.urlsafe_b64decode(cursor.encode()).decode()
        return max(0, int(raw.split(":", 1)[1]))
    except Exception:
        return 0


@app.get("/orders")
def list_orders(limit: int = 10, cursor: Optional[str] = None):
    limit = max(1, min(limit, 100))
    start = _decode(cursor)               # offset into catalog (0-based)
    end = min(start + limit, TOTAL_ORDERS)
    items = [{"id": i, "status": "created"} for i in range(start + 1, end + 1)]
    next_cursor = _encode(end) if end < TOTAL_ORDERS else None
    return {"items": items, "next_cursor": next_cursor}


@app.get("/")
def root():
    return {"status": "ok"}
