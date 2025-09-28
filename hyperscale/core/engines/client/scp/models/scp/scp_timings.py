from typing import Literal

SCPTimings = Literal[
    "request_start",
    "connect_start",
    "connect_end",
    "initialization_start",
    "initialization_end",
    "transfer_start",
    "transfer_end",
    "request_end",
]