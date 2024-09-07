from .canvas import Canvas
from .engine_config import EngineConfig


class RenderEngine:
    def __init__(self, config: EngineConfig) -> None:
        self.config = config
        self.canvas = Canvas(config.width, config.height)
