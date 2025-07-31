from .constants import (
    FXF_APPEND,
    FXF_APPEND_DATA,
    FXF_CREAT,
    FXF_CREATE_NEW,
    FXF_CREATE_TRUNCATE,
    FXF_EXCL,
    FXF_OPEN_EXISTING,
    FXF_OPEN_OR_CREATE,
    FXF_READ,
    FXF_TRUNC,
    FXF_TRUNCATE_EXISTING,
    FXF_WRITE,
    ACE4_READ_DATA,
    ACE4_READ_ATTRIBUTES,
    ACE4_WRITE_ATTRIBUTES,
    ACE4_WRITE_DATA,
    ACE4_APPEND_DATA,
)


def pflags_to_flags(pflags: int) -> tuple[int, int]:
    """Convert SFTPv3 pflags to SFTPv5 desired-access and flags"""

    desired_access = 0
    flags = 0

    if pflags & (FXF_CREAT | FXF_EXCL) == (FXF_CREAT | FXF_EXCL):
        flags = FXF_CREATE_NEW
    elif pflags & (FXF_CREAT | FXF_TRUNC) == (FXF_CREAT | FXF_TRUNC):
        flags = FXF_CREATE_TRUNCATE
    elif pflags & FXF_CREAT:
        flags = FXF_OPEN_OR_CREATE
    elif pflags & FXF_TRUNC:
        flags = FXF_TRUNCATE_EXISTING
    else:
        flags = FXF_OPEN_EXISTING

    if pflags & FXF_READ:
        desired_access |= ACE4_READ_DATA | ACE4_READ_ATTRIBUTES

    if pflags & FXF_WRITE:
        desired_access |= ACE4_WRITE_DATA | ACE4_WRITE_ATTRIBUTES

    if pflags & FXF_APPEND:
        desired_access |= ACE4_APPEND_DATA
        flags |= FXF_APPEND_DATA

    return desired_access, flags