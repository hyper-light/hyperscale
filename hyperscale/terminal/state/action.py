import inspect


from pydantic import validate_call, StrictStr, StrictInt, StrictFloat, ConfigDict, ValidationError
from typing import List, Dict, Callable, Awaitable, Tuple, TypeVar


ActionData = StrictInt | StrictFloat | StrictStr | List[StrictInt] | List[StrictFloat] | List[StrictStr]

Exception
T = TypeVar('T', bound=ActionData)

Action = Callable[[T, T], Awaitable[T] | T]




def action(func: Action[T]):

    if inspect.isawaitable(func):
    
        @validate_call(validate_return=True, config=ConfigDict(arbitrary_types_allowed=True))
        async def wrapper(previous: T, next: T) -> Awaitable[T]:
            try:
                return await func(previous, next)
            
            except ValidationError:
                return previous
        
        return wrapper
    
    else:

        @validate_call(validate_return=True, config=ConfigDict(arbitrary_types_allowed=True))
        def wrapper(previous: T, next: T) -> T:
            try:
                return func(previous, next)
            
            except ValidationError:
                return previous
        
        return wrapper
