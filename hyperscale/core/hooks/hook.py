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
from hyperscale.core.engines.client.custom import CustomResult
from hyperscale.core.engines.client.shared.models import (
    CallResult,
    RequestType,
)
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from hyperscale.reporting.models.metric import (
    COUNT,
    DISTRIBUTION,
    RATE,
    SAMPLE,
)
from hyperscale.core.snowflake.snowflake_generator import SnowflakeGenerator
from hyperscale.core.testing.models.base import OptimizedArg

from .hook_type import HookType
from .wrap_check import wrap_check
from .wrap_metric import wrap_metric


class Hook:
    def __init__(
        self,
        call: Callable[..., Awaitable[Any] | Awaitable[CallResult]],
        dependencies: List[str],
        timeouts: Optional[Timeouts] = None,
        tags: Optional[List[str]] = None,
    ) -> None:
        if timeouts is None:
            timeouts = Timeouts()

        id_generator = SnowflakeGenerator(
            (uuid.uuid1().int + threading.get_native_id()) >> 64
        )

        call_signature = signature(call)
        params = call_signature.parameters

        param_types = get_type_hints(call)

        if tags is None:
            tags: List[str] = []

        self.tags = tags
        self.kwarg_names = [arg.name for arg in params.values() if arg.KEYWORD_ONLY]

        self.optimized_args: Dict[str, OptimizedArg] = {
            arg.name: arg.default
            for arg in params.values()
            if arg.KEYWORD_ONLY
            and arg.name != "self"
            and isinstance(arg.default, OptimizedArg)
        }

        self.context_args = {
            arg.name: param_types.get(arg.name)(arg.default)
            for arg in params.values()
            if arg.KEYWORD_ONLY
            and arg.name != "self"
            and param_types.get(arg.name)
            and param_types.get(arg.name) in OptimizedArg.__subclasses__()
            and not arg.default == inspect._empty
            and arg.default is not None
        }

        self.optimized_args.update(self.context_args)

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

        self._call = call
        self.full_name = call.__qualname__
        self.name = call.__name__
        self.workflow = self.full_name.split(".").pop(0)
        self.dependencies = dependencies
        self.timeouts = timeouts
        self.call_id: int = id_generator.generate()
        self.custom_result_type_name: str | None = None

        self.static = True
        self.return_type = param_types.get("return")
        self.engine_type: Optional[RequestType] = None
        self.metric_type: Optional[COUNT | DISTRIBUTION | SAMPLE | RATE] = None

        annotation_subtypes = list(get_args(self.return_type))

        if len(annotation_subtypes) > 0:
            annotation_subtypes = [return_type for return_type in annotation_subtypes]

        is_test = self.return_type in CallResult.__subclasses__() or (
            self.return_type in CustomResult.__subclasses__()
        ) or (
            len(
                [
                    return_type
                    for return_type in annotation_subtypes
                    if return_type in CustomResult.__subclasses__()
                ]
            )
            > 0

        ) or (
            len(
                [
                    return_type
                    for return_type in annotation_subtypes
                    if return_type in CallResult.__subclasses__()
                ]
            )
            > 0
        )

        is_check = (
            self.return_type is Exception
            or (
                Exception in annotation_subtypes
                and type(None) in annotation_subtypes
                and len(annotation_subtypes) == 2
            )
            or (Exception in annotation_subtypes and len(annotation_subtypes) == 1)
        )

        metric_type = [
            return_type
            for return_type in annotation_subtypes
            if return_type
            in [
                COUNT,
                SAMPLE,
                DISTRIBUTION,
                RATE,
            ]
        ]

        is_metric = len(metric_type) > 0

        if is_test and self.return_type in CallResult.__subclasses__():
            self.hook_type = HookType.TEST
            self.engine_type: RequestType = self.return_type.response_type()

            self.call = self._call

        elif is_test and self.return_type in CustomResult.__subclasses__():
            self.hook_type = HookType.TEST
            self.engine_type: RequestType = self.return_type.response_type()
            self.custom_result_type_name = self.return_type.__name__

            self.call = self._call

        elif is_check:
            self.hook_type = HookType.CHECK
            self.call = wrap_check(self._call)

        elif is_metric:
            self.hook_type = HookType.METRIC
            self.metric_type = metric_type.pop()

            if self.metric_type == RATE:
                self.call = wrap_metric(self._call)

            else:
                self.call = self._call

        else:
            self.hook_type = HookType.ACTION
            self.call = self._call
