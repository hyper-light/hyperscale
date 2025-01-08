from __future__ import annotations
import asyncio
import re
from typing import Generic, TypeVar, Any, get_args


T = TypeVar('T')
K = TypeVar('K')


def reduce_pattern_type(pattern: Any):
    if isinstance(pattern, tuple) and len(pattern) > 0:
        for arg in pattern:
            return reduce_pattern_type(arg)
        
    elif (
        args := get_args(pattern)
    ) and len(args) > 0:
        return reduce_pattern_type(args)
    
    else:
        return pattern


class Pattern(Generic[T, K]):

    def  __init__(
        self,
        pattern: Pattern[tuple[T, K]]
    ):

        super().__init__()

        pattern_type, conversion_type = get_args(pattern)
        self._pattern = re.compile(reduce_pattern_type(pattern_type))

        conversion_type = reduce_pattern_type(conversion_type)
        self._data_type = [
            conversion_type.__name__
        ]

        self._types = tuple([
            conversion_type
        ])
        self._loop = asyncio.get_event_loop()

    def __contains__(self, value: Any):
        return type(value) in self._types

    @property
    def data_type(self):
        return str.__name__
    
    async def parse(self, arg: str):
        try:
            
            if value := re.match(self._pattern, arg):
                return self._parse_match(value.group(0))
            
        except Exception as e:
            return e

        return Exception('Err. - No match found.')
    
    def _parse_match(self, value: str):

        parse_error: Exception | None = None

        for subtype in self._types:
            try:
                if subtype == bytes:
                    return bytes(value, encoding='utf-8')
                
                else:
                    return subtype(value)
            
            except Exception as e:
                parse_error = e

        return parse_error

    
