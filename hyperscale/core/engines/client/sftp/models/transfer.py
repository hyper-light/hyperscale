from typing import Literal
from .file_attributes import FileAttributes


Listing = dict[
    Literal[
        "file_path",
        "file_type",
        "file_attributes",
        "file_transfer_at_end",
    ]
]


Transfer = dict[
    Literal[
        "file_path",
        "file_type",
        "file_data",
        "file_attributes",
        "file_transfer_at_end",
        "file_listing",
    ],
    bytes | str | FileAttributes | bool | list[Listing] | None
]