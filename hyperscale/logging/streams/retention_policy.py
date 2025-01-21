import datetime
import glob
import os
import pathlib
import re
from typing import Dict, Literal

from hyperscale.logging.rotation import (
    FileSizeParser,
    TimeParser,
)

RetentionPolicyConfig = Dict[
    Literal[
        "max_age",
        "rotation_time",
        "max_size"
    ],
    str
]

ParsedRetentionPolicyConfig = Dict[
    Literal[
        "max_age",
        "rotation_time",
        "max_size"
    ],
    int |float | str
    
]

CheckState = Dict[
    Literal[
        "max_age",
        "rotation_time",
        "max_size"
    ],
    bool
    
]

PolicyQuery = Dict[
    Literal[
        'file_age',
        'file_size',
        'logfile_path'
    ],
    int |float | pathlib.Path
]


def get_timestamp(filenamne: str):
    if match := re.match(
        r'[+-]?([0-9]*[.])?[0-9]+',
        filenamne
    ):
        return float(match.group(0))
    
    return 0


class RetentionPolicy:

    def __init__(
        self,
        retention_policy: RetentionPolicyConfig
    ) -> None:
        self._retention_policy = retention_policy
        self._parsed_policy: ParsedRetentionPolicyConfig = {}

        self._time_parser = TimeParser()
        self._file_size_parser = FileSizeParser()


    def parse(self):
        if max_age := self._retention_policy.get("max_age"):
            self._parsed_policy["max_age"] = self._time_parser.parse(max_age)

        if max_size := self._retention_policy.get('max_size'):
            self._parsed_policy['max_size'] = self._file_size_parser.parse(max_size)

        if rotation_time := self._retention_policy.get('rotation_time'):
            self._parsed_policy["rotation_time"] = rotation_time

    def matches_policy(
        self,
        policy_query: PolicyQuery,
    ):
        checks: CheckState = {
            "file_age": True,
            "file_size": True,
            "rotation_time": True,
        }

        resolved_path: pathlib.Path = policy_query["logfile_path"]
        logfile_directory = str(resolved_path.parent.absolute().resolve())
        
        if (
            file_age := policy_query['file_age']
        ) and (
            max_age := self._parsed_policy.get("max_age") 
        ):
            checks["file_age"] = file_age < max_age

        if (
            file_size := policy_query['file_size']
        ) and (
            max_file_size := self._parsed_policy.get("max_size")
        ):
            checks["file_size"] = file_size < max_file_size
            
        if rotation_time := self._parsed_policy.get("rotation_time"):
            last_archived: datetime.datetime | None = None
            existing_logfiles = glob.glob(
                os.path.join(
                    logfile_directory,
                    f'{resolved_path.stem}_*_archived.zst'
                )
            )


            if len(existing_logfiles) > 0:
                last_archived = datetime.datetime.fromtimestamp(
                    os.path.getmtime(max(
                        existing_logfiles,
                        key=os.path.getmtime,
                    )),
                )

            current_time = datetime.datetime.now()
            current_time_string = current_time.strftime('%H:%M')

            if last_archived and (current_time - last_archived).seconds < 60:
                checks["rotation_time"] = True

            else:
                checks["rotation_time"] = rotation_time != current_time_string

        return len([
            check for check in checks.values() if check is True
        ]) >= len(self._parsed_policy)
    
