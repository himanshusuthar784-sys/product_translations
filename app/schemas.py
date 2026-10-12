import re
import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

LANG_CODE_RE = re.compile(r"^[A-Za-z]{2,3}(-[A-Za-z0-9]{2,8})*$")  # en, fr, pt-BR, zh-Hant


# ----------------------------------------------------------------------------- languages
class LanguageIn(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    language_code: str = Field(min_length=2, max_length=16)
    is_active: bool = True

    @field_validator("language_code")
    @classmethod
    def _valid_code(cls, v: str) -> str:
        if not LANG_CODE_RE.match(v):
            raise ValueError("language_code must look like 'en', 'fr' or 'pt-BR'")
        head, *rest = v.split("-")
        return "-".join([head.lower(), *rest])


class LanguageOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    language_code: str
    is_default: bool
    is_active: bool


# ------------------------------------------------------------------------------ migration
class StartMigrationRequest(BaseModel):
    language_code: str = Field(min_length=2, max_length=16, examples=["fr"])
    fields: list[str] | None = Field(default=None, description="subset of translatable fields; default: all")
    batch_size: int | None = Field(default=None, ge=1, description="records per batch; default from config")
    idempotency_key: str | None = Field(default=None, max_length=128)


class StartMigrationResponse(BaseModel):
    job_id: uuid.UUID
    status: str
    language_code: str
    job_type: str
    created: bool = Field(description="false if an existing job was returned for the idempotency key")


class BatchCounts(BaseModel):
    total: int = 0
    pending: int = 0
    running: int = 0
    succeeded: int = 0
    failed: int = 0


class JobStatusResponse(BaseModel):
    job_id: uuid.UUID
    status: str
    language_code: str
    job_type: str
    processed_records: int
    total_records: int
    failed_records: int
    progress_percentage: float
    batches: BatchCounts
    batch_size: int
    fields: list[str]
    provider: str | None = None
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    elapsed_seconds: float | None = None
    eta_seconds: float | None = None
    error: str | None = None


class JobSummary(BaseModel):
    job_id: uuid.UUID
    status: str
    language_code: str
    job_type: str
    processed_records: int
    total_records: int
    progress_percentage: float
    created_at: datetime
    finished_at: datetime | None = None


class JobList(BaseModel):
    items: list[JobSummary]
    limit: int
    offset: int


class BatchOut(BaseModel):
    batch_no: int
    status: str
    range_start: int
    range_end: int
    expected_count: int
    processed_count: int
    attempts: int
    locked_by: str | None = None
    error: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None


class BatchList(BaseModel):
    items: list[BatchOut]
    limit: int
    offset: int


class ActionResponse(BaseModel):
    job_id: uuid.UUID
    status: str
    detail: str | None = None


# -------------------------------------------------------------------------------- products
class ProductOut(BaseModel):
    id: int
    shop_id: int
    product_id: int
    handle: str
    gender: str
    age_group: str
    variants_count: int
    is_deleted: bool
    language: str
    translations: dict[str, Any]
    fallback_keys: list[str] = Field(description="keys served from the default language")


class SetTranslationsRequest(BaseModel):
    translations: dict[str, Any] = Field(min_length=1, description='{"features": "...", "title": "..."}')

    @field_validator("translations")
    @classmethod
    def _keys(cls, v: dict[str, Any]) -> dict[str, Any]:
        for k in v:
            if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", k):
                raise ValueError(f"invalid translation key '{k}' (use snake_case, max 64 chars)")
        return v
