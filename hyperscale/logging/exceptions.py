"""WAL and LSN exceptions, each defined in its own module and re-exported
here so ``hyperscale.logging.exceptions`` stays their import path."""

from .lsn_generation_error import LSNGenerationError as LSNGenerationError
from .wal_backpressure_error import WALBackpressureError as WALBackpressureError
from .wal_batch_overflow_error import WALBatchOverflowError as WALBatchOverflowError
from .wal_closing_error import WALClosingError as WALClosingError
from .wal_consumer_too_slow_error import WALConsumerTooSlowError as WALConsumerTooSlowError
from .wal_error import WALError as WALError
from .wal_write_error import WALWriteError as WALWriteError
