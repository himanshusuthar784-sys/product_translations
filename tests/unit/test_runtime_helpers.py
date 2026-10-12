"""Pure helpers of the job engine: failure classification, backoff, progress maths."""
import pytest
from sqlalchemy.exc import DBAPIError, IntegrityError, InterfaceError, OperationalError, ProgrammingError

from app.models.enums import JobStatus
from app.services import job_runtime as rt
from app.services.job_api import _percentage
from app.workers.errors import LostLockError, TransientError


class TestIsTransient:
    @pytest.mark.parametrize(
        "exc",
        [
            TransientError("provider rate limited"),
            OperationalError("SELECT 1", {}, Exception("server closed the connection")),
            OperationalError("UPDATE", {}, Exception("deadlock detected")),
            InterfaceError("SELECT 1", {}, Exception("connection is closed")),
            DBAPIError("SELECT 1", {}, Exception("boom"), connection_invalidated=True),
        ],
    )
    def test_retryable(self, exc):
        assert rt.is_transient(exc) is True

    @pytest.mark.parametrize(
        "exc",
        [
            IntegrityError("INSERT", {}, Exception("duplicate key")),
            ProgrammingError("SELECT", {}, Exception("relation does not exist")),
            ValueError("provider returned wrong count"),
            KeyError("x"),
            LostLockError("not mine"),
            DBAPIError("SELECT 1", {}, Exception("x"), connection_invalidated=False),
        ],
    )
    def test_permanent(self, exc):
        assert rt.is_transient(exc) is False


class TestBackoff:
    def test_first_attempts_are_in_expected_jitter_band(self):
        for _ in range(200):
            assert 2.5 <= rt.backoff_seconds(1) <= 7.5     # 5s +-50%
            assert 5.0 <= rt.backoff_seconds(2) <= 15.0    # 10s +-50%
            assert 10.0 <= rt.backoff_seconds(3) <= 30.0   # 20s +-50%

    def test_is_capped(self):
        for _ in range(200):
            assert rt.backoff_seconds(50) <= 300 * 1.5

    def test_grows_on_average(self):
        avg = lambda a: sum(rt.backoff_seconds(a) for _ in range(300)) / 300  # noqa: E731
        assert avg(1) < avg(2) < avg(3) < avg(4)

    def test_attempt_zero_or_negative_does_not_blow_up(self):
        assert rt.backoff_seconds(0) > 0 and rt.backoff_seconds(-3) > 0

    def test_jitter_actually_varies(self):
        assert len({round(rt.backoff_seconds(3), 6) for _ in range(50)}) > 1


class TestWorkerId:
    def test_contains_host_and_pid(self):
        import os

        assert rt.worker_id().endswith(f":{os.getpid()}")


class TestPercentage:
    @pytest.mark.parametrize(
        "processed,total,status,expected",
        [
            (0, 10_000, JobStatus.RUNNING, 0.0),
            (6000, 10_000, JobStatus.RUNNING, 60.0),
            (3333, 10_000, JobStatus.RUNNING, 33.33),
            (10_000, 10_000, JobStatus.COMPLETED, 100.0),
            (12_000, 10_000, JobStatus.RUNNING, 100.0),       # never exceeds 100
            (0, 0, JobStatus.COMPLETED, 100.0),               # empty source: done
            (0, 0, JobStatus.PENDING, 0.0),                   # not planned yet
            (0, 0, JobStatus.FAILED, 0.0),
        ],
    )
    def test_percentage(self, processed, total, status, expected):
        assert _percentage(processed, total, status) == expected
