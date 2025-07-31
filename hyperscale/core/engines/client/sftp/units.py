import re
from typing import Mapping
from .constants import NSECS_IN_SEC


_unit_pattern = re.compile(r'([A-Za-z])')

time_units = {'': 1, 's': 1, 'm': 60, 'h': 60*60,
               'd': 24*60*60, 'w': 7*24*60*60}



def tuple_to_nsec(sec: int, nsec: int | None) -> int:
    """Convert seconds and remainder to nanoseconds since epoch"""

    return sec * NSECS_IN_SEC + (nsec or 0)


def tuple_to_float_sec(sec: int, nsec: int | None) -> float:
    """Convert seconds and remainder to float seconds since epoch"""

    return sec + float(nsec or 0) / NSECS_IN_SEC


def parse_units(value: str, suffixes: Mapping[str, int], label: str) -> float:
    """Parse a series of integers followed by unit suffixes"""

    matches = _unit_pattern.split(value)

    if matches[-1]:
        matches.append('')
    else:
        matches.pop()

    try:
        return sum(float(matches[i]) * suffixes[matches[i+1].lower()]
                   for i in range(0, len(matches), 2))
    except KeyError:
        raise ValueError('Invalid ' + label) from None
    


def parse_time_interval(value: str) -> float:
    """Parse a time interval with optional s, m, h, d, or w suffixes"""

    return parse_units(value, time_units, 'time interval')