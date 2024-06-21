import uuid
from typing import Any, Dict, List

from .engines.client import Client
from .hooks import Hook


class Workflow:
    def __init__(self):
        self.graph = __file__
        self.name = self.__class__.__name__
        self.id = str(uuid.uuid4())

        self.context: Dict[str, Any] = {}
        self.hooks: Dict[str, Hook] = {}

        self.client = Client(
            self.graph,
            self.id,
            self.name,
            self.id,
        )

        self.is_test: bool = False

        self.traversal_order: List[List[Hook]] = []
