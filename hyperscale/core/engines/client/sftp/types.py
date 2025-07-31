import asyncio
import ipaddress
from pathlib import PurePath
from typing import (
    Sequence, 
    Literal,
    Callable,
    IO,
    Type,
    AnyStr,
    TypeVar,
    Awaitable,
)
from .async_file_protocol import AsyncFileProtocol
from types import TracebackType

T = TypeVar('T')


MaybeAwait = T | Awaitable[T]

SFTPFileObj = IO[bytes]
SFTPPath = bytes | str | PurePath
SFTPPaths = SFTPPath | Sequence[SFTPPath]
SFTPPathList = list[bytes | list[bytes]]

SFTPErrorHandler = Literal[False] | Callable[[Exception], None] | None
SFTPProgressHandler = Callable[[bytes, bytes, int, int], None] | None

LocalPath = str | bytes


ExcInfo = tuple[Type[BaseException], BaseException, TracebackType]
OptExcInfo = ExcInfo | tuple[None, None, None]
SFTPOnErrorHandler = Callable[[Callable, bytes, OptExcInfo], None] | None

DataType = int | None



WaiterFuture = asyncio.Future[None]

RecvBuf = list[AnyStr | Exception]
RecvBufMap = dict[DataType, RecvBuf[AnyStr]]
ReadLocks = dict[DataType, asyncio.Lock]
ReadWaiters = dict[DataType, WaiterFuture | None]
DrainWaiters = dict[DataType, set[WaiterFuture]]

FilePath = str | PurePath

DefTuple = tuple[()] | T


IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address
IPNetwork = ipaddress.IPv4Network | ipaddress.IPv6Network


HostPort = tuple[str, int]
ListenKey = HostPort | str

SockAddr = tuple[str, int] | tuple[str, int, int, int]
File = IO[bytes] | AsyncFileProtocol[bytes]