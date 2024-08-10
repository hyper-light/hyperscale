import re
from datetime import timedelta


class TimeParser:
    def __init__(self) -> None:
        self._units = {
            "s": "seconds",
            "m": "minutes",
            "h": "hours",
            "d": "days",
            "w": "weeks",
        }

    def parse(self, time_amount: str):
        return float(
            timedelta(
                **{
                    self._units.get(
                        m.group("unit").lower(), 
                        "seconds"
                    ): float(
                        m.group("val")
                    )
                    for m in re.finditer(
                        r"(?P<val>\d+(\.\d+)?)(?P<unit>[smhdw]?)",
                        time_amount,
                        flags=re.I,
                    )
                }
            ).total_seconds()
        )
