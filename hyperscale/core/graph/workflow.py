import threading
import uuid
import importlib
from typing import List, Callable, Awaitable

from hyperscale.core.engines.client import Client
from hyperscale.core.snowflake.snowflake_generator import SnowflakeGenerator
from hyperscale.reporting.custom import CustomReporter
from hyperscale.reporting.reporter import ReporterConfig, JSONConfig


ReporterConfigs = (
    ReporterConfig
    | List[ReporterConfig]
    | Callable[[], ReporterConfig]
    | Callable[[], List[ReporterConfig]]
    | Callable[[], Awaitable[ReporterConfig]]
    | Callable[[], Awaitable[List[ReporterConfig]]]
)


class Workflow:
    vus: int = 1000
    duration: str = "1m"
    timeout: str = "30s"
    interval: str | None = None
    reporting: ReporterConfigs | CustomReporter | None = None

    def __init__(self):
        module = importlib.import_module(self.__module__)
        self.graph = module.__file__

        self.name = self.__class__.__name__

        generator = SnowflakeGenerator(
            (uuid.uuid1().int + threading.get_native_id()) >> 64
        )

        self.id = generator.generate()


        self.client = Client()

        if self.reporting is None:
            self.reporting = JSONConfig()
        
        if isinstance(self.timeout, str) is False:
            self.timeout = '30s'
