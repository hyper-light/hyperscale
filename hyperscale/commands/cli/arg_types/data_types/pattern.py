from __future__ import annotations
import asyncio
import re
from typing import Generic, TypeVar, Any, get_args, get_origin
from .reduce_pattern_type import reduce_pattern_type


T = TypeVar('T')
K = TypeVar('K')


class Pattern(Generic[T, K]):

    def  __init__(
        self,
        pattern: Pattern[T, K]
    ):

        super().__init__()

        self.data: K | None = None

        pattern_type, conversion_type = get_args(pattern)
        self._pattern = re.compile(reduce_pattern_type(pattern_type))

        self._data_type = conversion_type.__name__ if hasattr(conversion_type, '__name__') else type(conversion_type).__name__

        self._type = conversion_type

        self._loop = asyncio.get_event_loop()

    def __contains__(self, value: Any):
        return type(value) in [self._type]

    @property
    def data_type(self):
        return self._data_type
    
    async def parse(self, arg: str | None = None):

        results: Any | Exception = await self._try_match(arg)

        if isinstance(results, Exception):
            return results

        self.data: K = results

        return self

    async def _try_match(self, arg: str):

        try:

            if get_origin(self._type) == list:

                args_type = get_args(self._type)
                parse_type = args_type[0] if len(args_type) > 0 else None

                return self._parse_match_all(
                    re.findall(
                        self._pattern, 
                        arg,
                    ),
                    parse_type=parse_type
                )
            
            elif value := re.match(self._pattern, arg):
                return self._parse_match(
                    value.group(0)
                )

        except Exception as e:
            return e
    
    def _parse_match_all(
        self, 
        value: list[str],
        parse_type: type[Any] | None = None
    ):
        results: list[K] = []

        for item in value:
            result = self._parse_match(
                item,
                parse_type=parse_type,
            )

            if not isinstance(results, Exception):
                results.append(result)
        
        if len(results) < 1:
            return Exception('Err. - no matches found')
        
        return results
    
    def _parse_match(
        self, 
        value: str,
        parse_type: type[Any] | None = None
    ):
        
        parser = self._type
        if parse_type:
            parser = parse_type

        try:
            if parser == bytes:
                return bytes(value, encoding='utf-8')
            
            return parser(value)
        
        except Exception as e:
            return e

        

    
