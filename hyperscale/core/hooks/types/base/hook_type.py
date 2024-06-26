from enum import Enum


class HookType(Enum):
    ACTION = "ACTION"
    CHANNEL = "CHANNEL"
    CHECK = "CHECK"
    CONTEXT = "CONTEXT"
    CONDITION = "CONDITION"
    EVENT = "EVENT"
    INTERNAL = "INTERNAL"
    METRIC = "METRIC"
    LOAD = "LOAD"
    SAVE = "SAVE"
    TASK = "TASK"
    TRANSFORM = "TRANSFORM"
