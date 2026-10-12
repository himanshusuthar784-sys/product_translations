import time
import uuid
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.api.routes import health, languages, migration, products
from app.core.db import make_async_engine, make_async_sessionmaker
from app.core.logging import configure_logging, get_logger
from app.services.errors import ApiError

configure_logging()
log = get_logger("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    engine = make_async_engine()
    app.state.engine = engine
    app.state.sessionmaker = make_async_sessionmaker(engine)
    log.info("api_started")
    yield
    await engine.dispose()  # graceful: returns pooled connections on SIGTERM
    log.info("api_stopped")


app = FastAPI(
    title="Product Translations Migration API",
    version="1.0.0",
    description="Stateless API: multi-language product translations + async, resumable migration jobs.",
    lifespan=lifespan,
)


@app.middleware("http")
async def request_context(request: Request, call_next):
    rid = request.headers.get("x-request-id") or uuid.uuid4().hex
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(request_id=rid)
    started = time.monotonic()
    try:
        response = await call_next(request)
    except Exception:
        log.error("request_failed", method=request.method, path=request.url.path, exc_info=True)
        raise
    response.headers["x-request-id"] = rid
    if request.url.path not in ("/health",):
        log.info(
            "request",
            method=request.method,
            path=request.url.path,
            status=response.status_code,
            ms=round((time.monotonic() - started) * 1000, 1),
        )
    return response


@app.exception_handler(ApiError)
async def api_error_handler(_: Request, exc: ApiError):
    return JSONResponse(
        status_code=exc.status_code, content={"error": {"code": exc.code, "message": exc.message, **exc.extra}}
    )


@app.exception_handler(RequestValidationError)
async def validation_handler(_: Request, exc: RequestValidationError):
    # Keep only loc/msg/type: always JSON-serialisable (raw errors() may carry exception objects or echo input).
    details = [{"loc": list(e.get("loc", ())), "msg": e.get("msg", ""), "type": e.get("type", "")} for e in exc.errors()]
    return JSONResponse(
        status_code=422,
        content={"error": {"code": "validation_error", "message": "invalid request", "details": details}},
    )


app.include_router(health.router)
app.include_router(migration.router)
app.include_router(languages.router)
app.include_router(products.router)
