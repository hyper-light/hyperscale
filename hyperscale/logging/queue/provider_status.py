from enum import Enum


class ProviderStatus(Enum):
    READY='READY'
    RUNNING='RUNNING'
    CLOSING='CLOSING'
    CLOSED='CLOSED'