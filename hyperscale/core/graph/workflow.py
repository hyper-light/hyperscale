import threading
import uuid
import importlib


from hyperscale.core.engines.client import Client
from hyperscale.core.snowflake.snowflake_generator import SnowflakeGenerator


class Workflow:
    vus = 1000
    duration = "1m"

    def __init__(self):
        module = importlib.import_module(self.__module__)
        self.graph = module.__file__

        self.name = self.__class__.__name__

        generator = SnowflakeGenerator(
            (uuid.uuid1().int + threading.get_native_id()) >> 64
        )

        self.id = generator.generate()

        self.client = Client()
