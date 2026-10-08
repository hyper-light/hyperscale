"""
Whether a node's durable storage can take the writes it needs.

Every durable store a node keeps under its data directory -- the job
ledger's WAL, the idempotency ledger, persisted submission payloads --
lives on one device, so one tracker per node records each write's
outcome.

On a nearly full device whether a write succeeds depends on its size:
a small record still fits after a large payload was refused. "The last
write succeeded" therefore flaps with write size and cannot answer the
question placement asks (can this node durably accept a job?). Storage
is unwritable from a failed write until a write AT LEAST AS LARGE as
the largest one refused since has succeeded -- the owner's probe
writes exactly that many bytes to prove it.
"""


class StorageHealth:
    """Tracks the largest refused write not yet proven to fit again."""

    __slots__ = ("_unproven_bytes", "_last_failure")

    def __init__(self) -> None:
        self._unproven_bytes: int | None = None
        self._last_failure: OSError | None = None

    def record_success(self, bytes_written: int) -> None:
        """A durable write of ``bytes_written`` bytes completed."""
        if self._unproven_bytes is not None and bytes_written >= self._unproven_bytes:
            self._unproven_bytes = None

    def record_failure(self, error: OSError, bytes_attempted: int) -> None:
        """A durable write of ``bytes_attempted`` bytes failed on storage
        (full, read-only, I/O error)."""
        self._last_failure = error
        self._unproven_bytes = max(self._unproven_bytes or 0, bytes_attempted)

    @property
    def writable(self) -> bool:
        return self._unproven_bytes is None

    @property
    def unproven_bytes(self) -> int | None:
        """The size a write must reach to prove storage writable again
        (None while writable)."""
        return self._unproven_bytes

    @property
    def last_failure(self) -> OSError | None:
        """The latest storage failure, or None if none ever happened."""
        return self._last_failure
