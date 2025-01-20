from enum import Enum


class ConsumerStatus(Enum):
    READY = 'READY'
    RUNNING = 'RUNNING'
    CLOSING = 'CLOSING'
    ABORTING = 'ABORTING'
    CLOSED = 'CLOSED'
    