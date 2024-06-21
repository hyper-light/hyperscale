import threading
import uuid
from typing import Any, Dict

from hyperscale.core_rewrite.snowflake.snowflake_generator import SnowflakeGenerator

from .engines.client import Client
from .hooks import Hook


class Workflow:
    def __init__(self):
        self.graph = __file__
        self.name = self.__class__.__name__

        generator = SnowflakeGenerator(
            (uuid.uuid1().int + threading.get_native_id()) >> 64
        )

        self.id = generator.generate()

        self.context: Dict[str, Any] = {}
        self.hooks: Dict[str, Hook] = {}

        self.client = Client(
            self.graph,
            self.id,
            self.name,
            self.id,
        )
