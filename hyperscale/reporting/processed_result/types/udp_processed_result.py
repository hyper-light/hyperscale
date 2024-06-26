import json
from typing import Dict, Union

from hyperscale.core.engines.types.udp import UDPResult

from .base_processed_result import BaseProcessedResult


class UDPProcessedResult(BaseProcessedResult):
    __slots__ = (
        "event_id",
        "action_id",
        "url",
        "ip_addr",
        "path",
        "method",
        "headers",
        "params",
        "hostname",
        "status",
        "data",
        "timings",
    )

    def __init__(self, stage: str, result: UDPResult) -> None:
        super(UDPProcessedResult, self).__init__(stage, result)

        self.url = result.url
        self.ip_addr = result.ip_addr
        self.path = result.path
        self.method = "n/a"
        self.headers = {}
        self.params = result.params
        self.hostname = result.hostname
        self.status = result.status
        self.data = result.data
        self.name = self.shortname

        self.time = result.complete - result.start

        self.timings = {
            "total": self.time,
            "waiting": result.start - result.wait_start,
            "connecting": result.connect_end - result.start,
            "writing": result.write_end - result.connect_end,
            "reading": result.complete - result.write_end,
        }

    def to_dict(self) -> Dict[str, Union[str, int, float]]:
        data = self.data
        if isinstance(data, (bytes, bytearray)):
            data = data.decode()

        return {
            "name": self.name,
            "stage": self.stage,
            "shortname": self.shortname,
            "checks": [check.__name__ for check in self.checks],
            "error": str(self.error),
            "time": self.time,
            "type": self.type,
            "source": self.source,
            "url": self.url,
            "ip_addr": self.ip_addr,
            "method": self.method,
            "path": self.path,
            "params": self.params,
            "hostname": self.hostname,
            "status": self.status,
            "headers": self.headers,
            "data": data,
        }

    def serialize(self) -> str:
        data = self.data
        if isinstance(data, (bytes, bytearray)):
            data = data.decode()

        return json.dumps(self.to_dict())
