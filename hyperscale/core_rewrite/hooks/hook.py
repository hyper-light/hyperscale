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
from hyperscale.core_rewrite.snowflake.snowflake_generator import SnowflakeGenerator

from .optimized.models import (
    URL,
    Auth,
    Cookies,
    Data,
    Headers,
    Mutation,
    Params,
    Protobuf,
    Query,
)
from .optimized.models.base import OptimizedArg


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

        self.optimized_args: Dict[
            str,
            URL
            | Auth
            | Cookies
            | Data
            | Headers
            | Mutation
            | Params
            | Protobuf
            | Query,
        ] = {
            arg.name: arg.default
            for arg in params.values()
            if arg.KEYWORD_ONLY
            and arg.name != "self"
            and isinstance(arg.default, OptimizedArg)
        }

        for arg in self.optimized_args.values():
            arg.call_name = call.__name__

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
