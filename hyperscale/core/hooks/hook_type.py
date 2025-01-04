from enum import Enum


class HookType(Enum):
    TEST = "TEST"
    CHECK = "CHECK"
    METRIC = "METRIC"
    ACTION = "ACTION"
