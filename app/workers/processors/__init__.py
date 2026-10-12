from app.models.enums import JobType
from app.workers.processors.base import BatchContext, BatchProcessor
from app.workers.processors.import_base import ImportBaseProcessor
from app.workers.processors.translate import TranslateProcessor

_PROCESSORS: dict[JobType, type[BatchProcessor]] = {
    JobType.IMPORT_BASE: ImportBaseProcessor,
    JobType.TRANSLATE: TranslateProcessor,
}


def get_processor(job_type: JobType) -> BatchProcessor:
    return _PROCESSORS[job_type]()


__all__ = ["BatchContext", "BatchProcessor", "get_processor"]
