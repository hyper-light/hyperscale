from enum import Enum


class HookType(Enum):
    SEND = "SEND"
    PUSH = "PUSH"
    RECEIVE = "RECEIVE"
    TASK = "TASK"
    BROADCAST = "BROADCAST"
