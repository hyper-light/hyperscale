import json
from typing import Any, Dict, Iterator, List, Union
from urllib.parse import urlencode

from hyperscale.core.engines.types.common import URL
from hyperscale.core.engines.types.common.base_action import BaseAction
from hyperscale.core.engines.types.common.hooks import Hooks
from hyperscale.core.engines.types.common.protocols.shared.writer import Writer
from hyperscale.core.engines.types.common.types import RequestTypes


class UDPAction(BaseAction):
    __slots__ = (
        "action_id",
        "protocols",
        "wait_for_response",
        "type",
        "url",
        "_data",
        "encoded_data",
        "is_stream",
        "ssl_context",
        "event",
        "action_args",
        "mutations",
    )

    def __init__(
        self,
        name: str,
        url: str,
        wait_for_response: bool = False,
        data: Union[str, dict, Iterator, bytes, None] = None,
        user: str = None,
        tags: List[Dict[str, str]] = [],
    ) -> None:
        super(UDPAction, self).__init__(name, user, tags)

        self.wait_for_response = wait_for_response
        self.type = RequestTypes.UDP

        address_family, protocol = self.protocols[self.type]
        self.url = URL(url, family=address_family, protocol=protocol)

        self._data = data

        self.encoded_data = None
        self.is_stream = False
        self.ssl_context = None
        self.hooks: Hooks[UDPAction] = Hooks()
        self.action_args: Dict[str, Any] = {}

    @property
    def size(self):
        if self.encoded_data:
            return len(self.encoded_data)

        else:
            return 0

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, value):
        self._data = value
        self.encoded_data = None

    def setup(self):
        if self.encoded_data is None:
            self._setup_data()

    def _setup_data(self):
        if self._data:
            if isinstance(self._data, Iterator):
                chunks = bytearray()
                for chunk in self._data:
                    chunks.extend(chunk.encode())

                self.is_stream = True
                self.encoded_data = bytes(chunks)

            else:
                if isinstance(self._data, dict):
                    self.encoded_data = json.dumps(self._data).encode()

                elif isinstance(self._data, tuple):
                    self.encoded_data = urlencode(self._data).encode()

                elif isinstance(self._data, str):
                    self.encoded_data = self._data.encode()

    def write_chunks(self, writer: Writer):
        for chunk in self.data:
            writer.write(chunk)
