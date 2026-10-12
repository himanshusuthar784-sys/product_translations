import asyncio

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_session
from app.core.config import get_settings

router = APIRouter(tags=["health"])


@router.get("/health")
async def health():
    """Liveness: the process is up. Touches no dependency, so a DB blip never restarts healthy pods."""
    return {"status": "ok"}


@router.get("/ready")
async def ready(request: Request, session: AsyncSession = Depends(get_session)):
    """Readiness: can this replica serve traffic (DB reachable, broker reachable)?"""
    checks: dict[str, str] = {}
    try:
        await asyncio.wait_for(session.execute(text("SELECT 1")), timeout=3)
        checks["postgres"] = "ok"
    except Exception as exc:  # noqa: BLE001
        checks["postgres"] = f"error: {type(exc).__name__}"
    client = aioredis.from_url(get_settings().redis_url, socket_connect_timeout=2, socket_timeout=2)
    try:
        await client.ping()
        checks["redis"] = "ok"
    except Exception as exc:  # noqa: BLE001
        checks["redis"] = f"error: {type(exc).__name__}"
    finally:
        await client.aclose()
    ok = all(v == "ok" for v in checks.values())
    return JSONResponse({"status": "ready" if ok else "unavailable", "checks": checks}, status_code=200 if ok else 503)
