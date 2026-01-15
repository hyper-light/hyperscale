class WALError(Exception):
    pass


class WALBackpressureError(WALError):
    pass


class WALWriteError(WALError):
    pass


class WALBatchOverflowError(WALError):
    pass


class WALConsumerTooSlowError(WALError):
    pass


class LSNGenerationError(WALError):
    pass


class WALClosingError(WALError):
    pass
