import threading
import uuid

from hyperscale.core_rewrite.engines.client import Client
from hyperscale.core_rewrite.snowflake.snowflake_generator import SnowflakeGenerator


class Workflow:
    vus = 1000
    duration = "1m"

    def __init__(self):
        self.graph = __file__
        self.name = self.__class__.__name__

        generator = SnowflakeGenerator(
            (uuid.uuid1().int + threading.get_native_id()) >> 64
        )

        self.id = generator.generate()

        self.client = Client()
