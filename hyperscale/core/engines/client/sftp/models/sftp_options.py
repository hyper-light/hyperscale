import pathlib
from typing import Literal
from functools import reduce
from operator import ior

from hyperscale.core.engines.client.ssh.protocol.ssh.constants import (
    ACE4_READ_DATA,
    ACE4_APPEND_DATA,
    ACE4_READ_ATTRIBUTES,
    FXRP_NO_CHECK,
    FXRP_STAT_IF_EXISTS,
    FXRP_STAT_ALWAYS,
    ACE4_WRITE_DATA,
    ACE4_WRITE_ATTRIBUTES,
    FILEXFER_ATTR_ACCESSTIME,
    FILEXFER_ATTR_ACL,
    FILEXFER_ATTR_ACMODTIME,
    FILEXFER_ATTR_ALLOCATION_SIZE,
    FILEXFER_ATTR_BITS,
    FILEXFER_ATTR_CREATETIME,
    FILEXFER_ATTR_CTIME,
    FILEXFER_ATTR_DEFINED_V3,
    FILEXFER_ATTR_DEFINED_V4,
    FILEXFER_ATTR_DEFINED_V5,
    FILEXFER_ATTR_DEFINED_V6,
    FILEXFER_ATTR_EXTENDED,
    FILEXFER_ATTR_LINK_COUNT,
    FILEXFER_ATTR_MIME_TYPE,
    FILEXFER_ATTR_MODIFYTIME,
    FILEXFER_ATTR_OWNERGROUP,
    FILEXFER_ATTR_PERMISSIONS,
    FILEXFER_ATTR_SIZE,
    FILEXFER_ATTR_SUBSECOND_TIMES,
    FILEXFER_ATTR_TEXT_HINT,
    FILEXFER_ATTR_UIDGID,
    FILEXFER_ATTR_UNTRANSLATED_NAME,

)


AttributeFlags = Literal[
    "size",
    "uid-gid",
    "permissions",
    "acmodtime",
    "extended",
    "defined-v3",
    "accesstime",
    "createtime",
    "modifytime",
    "acl",
    "ownergroup",
    "subsecond-times",
    "defined-v4",
    "bits",
    "defined-v5",
    "allocation-size",
    "text-hint",
    "mime-type",
    "link-count",
    "untranslated-name",
    "ctime",
    "defined-v6"
]
CheckType = Literal["none", "always", "exists"]
DesiredAccess = Literal["read", "write", "append", "read-attr", "write-attr"]


class SFTPOptions:

    __slots__ = (
        "check",
        "compose_paths",
        "desired_access",
        "exist_ok",
        "flags",
        "follow_symlinks",
        "gid",
        "group",
        "nanoseconds",
        "owner",
        "permissions",
        "recurse",
        "times",
        "uid",
        "_check_opt_map",
        "_desired_access_map",
        "_flags",
    )

    def __init__(
        self,
        recurse: bool = False,
        follow_symlinks: bool = False,
        # desired_access: int = ACE4_READ_DATA | ACE4_READ_ATTRIBUTES,
        exist_ok: bool = False,
        check: CheckType = "none",
        desired_access: list[DesiredAccess] | None = None,
        flags: list[AttributeFlags] | None = None,
        uid: int = 1000,
        gid: int = 1000,
        owner: str | None = None,
        group: str | None = None,
        permissions: int = 644,
        times: tuple[float, float] | None = None,
        nanoseconds: tuple[int, int] | None = None,
        compose_paths: list[str | pathlib.PurePath] | None = None
    ):
        self.recurse = recurse
        self.follow_symlinks = follow_symlinks
        self.exist_ok = exist_ok
        self.gid = gid
        self.group = group
        self.owner = owner
        self.permissions = permissions
        self.uid = uid
        self.nanoseconds = nanoseconds
        self.times = times

        if compose_paths is None:
            compose_paths = []

        
        self.compose_paths = compose_paths
        
        if desired_access is None:
            desired_access = ["read", "read-attr"]

        if flags is None:
            flags = ["defined-v4"]
        
        self._check_opt_map: dict[CheckType, int] = {
            "none": FXRP_NO_CHECK,
            "always": FXRP_STAT_ALWAYS,
            "exists": FXRP_STAT_IF_EXISTS,
        }

        self.check = self._check_opt_map[check]

    
        self._desired_access_map: dict[DesiredAccess, int] = {
            "read": ACE4_READ_DATA,
            "write": ACE4_WRITE_DATA,
            "append": ACE4_APPEND_DATA,
            "read-attr": ACE4_READ_ATTRIBUTES,
            "write-attr": ACE4_WRITE_ATTRIBUTES
        }

        self.desired_access: int = reduce(
            ior,
            [
                self._desired_access_map[access]
                for access in desired_access
            ]
        )

        self._flags: dict[
            AttributeFlags,
            int,
        ] = {
            "accesstime": FILEXFER_ATTR_ACCESSTIME,
            "acl": FILEXFER_ATTR_ACL,
            "acmodtime": FILEXFER_ATTR_ACMODTIME,
            "allocation-size": FILEXFER_ATTR_ALLOCATION_SIZE,
            "bits": FILEXFER_ATTR_BITS,
            "createtime": FILEXFER_ATTR_CREATETIME,
            "ctime": FILEXFER_ATTR_CTIME,
            "defined-v3": FILEXFER_ATTR_DEFINED_V3,
            "defined-v4": FILEXFER_ATTR_DEFINED_V4,
            "defined-v5": FILEXFER_ATTR_DEFINED_V5,
            "defined-v6": FILEXFER_ATTR_DEFINED_V6,
            "extended": FILEXFER_ATTR_EXTENDED,
            "link-count": FILEXFER_ATTR_LINK_COUNT,
            "mime-type": FILEXFER_ATTR_MIME_TYPE,
            "modifytime": FILEXFER_ATTR_MODIFYTIME,
            "ownergroup": FILEXFER_ATTR_OWNERGROUP,
            "permissions": FILEXFER_ATTR_PERMISSIONS,
            "size": FILEXFER_ATTR_SIZE,
            "subsecond-times": FILEXFER_ATTR_SUBSECOND_TIMES,
            "text-hint": FILEXFER_ATTR_TEXT_HINT,
            "uid-gid": FILEXFER_ATTR_UIDGID,
            "untranslated-name": FILEXFER_ATTR_UNTRANSLATED_NAME,
        }

        self.flags: int = reduce(
            ior,
            [
                self._flags[flag]
                for flag in flags
            ]
        )

