import inspect
import threading
import uuid
from inspect import signature
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    get_args,
    get_type_hints,
)

from hyperscale.core.engines.types.common.base_action import BaseAction
from hyperscale.core_rewrite.engines.client.shared.models import (
    CallResult,
    RequestType,
)
from hyperscale.core_rewrite.engines.client.shared.timeouts import Timeouts
from hyperscale.core_rewrite.optimized.models.base import OptimizedArg
from hyperscale.core_rewrite.snowflake.snowflake_generator import SnowflakeGenerator


class Hook:
    def __init__(
        self,
        call: Callable[
            ..., Awaitable[Any] | Awaitable[BaseAction] | Awaitable[CallResult]
        ],
        dependencies: List[str],
        timeouts: Optional[Timeouts] = None,
    ) -> None:
        if timeouts is None:
            timeouts = Timeouts()

        id_generator = SnowflakeGenerator(
            (uuid.uuid1().int + threading.get_native_id()) >> 64
        )

        call_signature = signature(call)
        params = call_signature.parameters

        param_types = get_type_hints(call)

        self.optimized_args: Dict[str, OptimizedArg] = {
            arg.name: arg.default
            for arg in params.values()
            if arg.KEYWORD_ONLY
            and arg.name != "self"
            and isinstance(arg.default, OptimizedArg)
        }

        self.optimized_args.update(
            {
                arg.name: param_types.get(arg.name)(arg.default)
                for arg in params.values()
                if arg.KEYWORD_ONLY
                and arg.name != "self"
                and param_types.get(arg.name)
                and not arg.default == inspect._empty
            }
        )

        for arg in self.optimized_args.values():
            arg.call_name = call.__name__

        self.optimized_slots: Dict[str, OptimizedArg] = {
            arg.name: arg.annotation
            for arg in params.values()
            if arg.POSITIONAL_OR_KEYWORD
            and arg.name != "self"
            and param_types.get(arg.name) in OptimizedArg.__subclasses__()
            and arg.name not in self.optimized_args
            and not isinstance(arg.default, inspect._empty)
        }

        self.call = call
        self.full_name = call.__qualname__
        self.name = call.__name__
        self.workflow = self.full_name.split(".").pop(0)
        self.dependencies = dependencies
        self.timeouts = timeouts
        self.call_id: int = id_generator.generate()

        self.static = True
        self.return_type = param_types.get("return")
        self.is_test = False
        self.engine_type: Optional[RequestType] = None

        annotation_subtypes = list(get_args(self.return_type))

        if len(annotation_subtypes) > 0:
            self.return_type = [return_type for return_type in annotation_subtypes]

        else:
            self.is_test = self.return_type in CallResult.__subclasses__()
            self.return_type: CallResult = self.return_type
            self.engine_type: RequestType = self.return_type.response_type()
