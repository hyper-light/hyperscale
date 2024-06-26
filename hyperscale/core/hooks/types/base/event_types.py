from enum import Enum


class EventType(Enum):
    ACTION = "ACTION"
    CHANNEL = "CHANNEL"
    CHECK = "CHECK"
    CONDITION = "CONDITION"
    CONTEXT = "CONTEXT"
    EVENT = "EVENT"
    LOAD = "LOAD"
    METRIC = "METRIC"
    SAVE = "SAVE"
    TASK = "TASK"
    TRANSFORM = "TRANSFORM"
