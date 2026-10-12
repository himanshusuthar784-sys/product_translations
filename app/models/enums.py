import enum


class TranslationSource(str, enum.Enum):
    """Declaration order == precedence (mirrors the PG enum): fallback < import < machine < human."""

    FALLBACK = "fallback"    # placeholder copied from the default language
    IMPORT = "import"        # imported from the legacy English data
    MACHINE = "machine"      # produced by an MT/LLM provider
    HUMAN = "human"          # edited by a person - never overwritten by jobs


class JobType(str, enum.Enum):
    IMPORT_BASE = "import_base"   # legacy -> products + default-language translations
    TRANSLATE = "translate"       # default-language translations -> target language


class JobStatus(str, enum.Enum):
    PENDING = "pending"
    PLANNING = "planning"
    RUNNING = "running"
    COMPLETED = "completed"
    COMPLETED_WITH_ERRORS = "completed_with_errors"
    FAILED = "failed"
    CANCELLED = "cancelled"


ACTIVE_JOB_STATUSES = (JobStatus.PENDING, JobStatus.PLANNING, JobStatus.RUNNING)
TERMINAL_JOB_STATUSES = (
    JobStatus.COMPLETED,
    JobStatus.COMPLETED_WITH_ERRORS,
    JobStatus.FAILED,
    JobStatus.CANCELLED,
)


class BatchStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
