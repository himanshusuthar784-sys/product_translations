class TransientError(Exception):
    """Retry-able failure (provider rate limit, timeout, ...). Providers should raise this."""


class LostLockError(Exception):
    """This worker no longer owns the batch (the reaper re-assigned it). Abort without side effects."""
