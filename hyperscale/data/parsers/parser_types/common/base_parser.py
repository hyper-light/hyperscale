import asyncio
import re
from typing import Any, Coroutine, Dict

from hyperscale.core.engines.client.config import Config
from hyperscale.core.engines.types.common.timeouts import Timeouts
from hyperscale.core.engines.types.common.types import RequestTypes
from hyperscale.core.hooks.types.action.hook import ActionHook


class BaseParser:

    def __init__(
        self, 
        name: str,
        config: Config,
        parser_type: RequestTypes,
        options: Dict[str, Any]={}
    ) -> None:
        
        self._loop: asyncio.AbstractEventLoop = None
        self._name_pattern = re.compile('[^0-9a-zA-Z]+')
        self.name = name
        self.config = config
        self.timeouts = Timeouts(
            connect_timeout=config.connect_timeout,
            total_timeout=config.request_timeout
        )
        
        self.parser_type = parser_type
        self.options = options

    async def parse(self, action_data: Dict[str, Any]) -> Coroutine[Any, Any, ActionHook]:
        raise NotImplementedError('Parse method is not implemented for base Parser class.')
    
