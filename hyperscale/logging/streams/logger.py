from __future__ import annotations

import asyncio
import datetime
import pathlib
import sys
import threading
from typing import (
    Callable,
    Dict,
    TypeVar,
    Any
)

from hyperscale.logging.models import Entry, Log

from .logger_context import LoggerContext
from .retention_policy import RetentionPolicyConfig

T = TypeVar('T', bound=Entry)


class Logger:
    def __init__(self) -> None:
        self._contexts: Dict[str, LoggerContext] = {}
        self._watch_tasks: Dict[str, asyncio.Task] = {}

    def __getitem__(self, name: str):

        if self._contexts.get(name) is None:
            self._contexts[name] = LoggerContext(name=name)

        return self._contexts[name]
    
    def get_stream(
        self,
        name: str | None = None,
        template: str | None = None,
        path: str | None = None,
        retention_policy: RetentionPolicyConfig | None = None,
        models: dict[
            str,
            tuple[
                type[T],
                dict[str, Any],
            ]
        ] | None = None,           
    ):
        if name is None:
            name = 'default'

        filename: str | None = None
        directory: str | None = None

        if path:
            logfile_path = pathlib.Path(path)
            is_logfile = len(logfile_path.suffix) > 0 

            filename = logfile_path.name if is_logfile else None
            directory = str(logfile_path.parent.absolute()) if is_logfile else str(logfile_path.absolute())

        self._contexts[name] = LoggerContext(
            name=name,
            template=template,
            filename=filename,
            directory=directory,
            retention_policy=retention_policy,
            models=models,
        )

        return self._contexts[name].stream
    
    def configure(
        self,
        name: str | None = None,
        template: str | None = None,
        path: str | None = None,
        retention_policy: RetentionPolicyConfig | None = None,
        models: dict[
            str,
            tuple[
                type[T],
                dict[str, Any],
            ]
        ] | None = None,
    ):
        if name is None:
            name = 'default'

        filename: str | None = None
        directory: str | None = None

        if path:
            logfile_path = pathlib.Path(path)
            is_logfile = len(logfile_path.suffix) > 0 

            filename = logfile_path.name if is_logfile else None
            directory = str(logfile_path.parent.absolute()) if is_logfile else str(logfile_path.absolute())

        self._contexts[name] = LoggerContext(
            name=name,
            template=template,
            filename=filename,
            directory=directory,
            retention_policy=retention_policy,
            models=models,
        )

    def context(
        self,
        name: str | None = None,
        template: str | None = None,
        path: str | None = None,
        retention_policy: RetentionPolicyConfig | None = None,
        nested: bool = False,
        models: dict[
            str,
            tuple[
                type[T],
                dict[str, Any],
            ]
        ] | None = None,
    ):
        if name is None:
            name = 'default'

        filename: str | None = None
        directory: str | None = None

        if path:
            logfile_path = pathlib.Path(path)
            is_logfile = len(logfile_path.suffix) > 0 

            filename = logfile_path.name if is_logfile else None
            directory = str(logfile_path.parent.absolute()) if is_logfile else str(logfile_path.absolute())

        if self._contexts.get(name) is None:

            self._contexts[name] = LoggerContext(
                name=name,
                template=template,
                filename=filename,
                directory=directory,
                retention_policy=retention_policy,
                nested=nested,
                models=models,
            )

        else:
            self._contexts[name].name = name if name else self._contexts[name].name
            self._contexts[name].template = template if template else self._contexts[name].template
            self._contexts[name].filename = filename if filename else self._contexts[name].filename
            self._contexts[name].directory = directory if directory else self._contexts[name].directory
            self._contexts[name].retention_policy = retention_policy if retention_policy else self._contexts[name].retention_policy
            self._contexts[name].nested = nested
            
        return self._contexts[name]
    
    async def subscribe(
        self, 
        logger: Logger,
        name: str | None = None,
        template: str | None = None,
        path: str | None = None,
        retention_policy: RetentionPolicyConfig | None = None,
        models: dict[
            str,
            tuple[
                type[T],
                dict[str, Any],
            ]
        ] | None = None,
    ):
        filename: str | None = None
        directory: str | None = None

        if name is None:
            name = 'default'

        if path:
            logfile_path = pathlib.Path(path)
            is_logfile = len(logfile_path.suffix) > 0 

            filename = logfile_path.name if is_logfile else None
            directory = str(logfile_path.parent.absolute()) if is_logfile else str(logfile_path.absolute())

        if self._contexts.get(name) is None:
            self._contexts[name] = LoggerContext(
                name=name,
                template=template,
                filename=filename,
                directory=directory,
                retention_policy=retention_policy,
                models=models,
            )

            await self._contexts[name].stream.initialize()

        if logger._contexts.get(name) is None:
            logger._contexts[name] = LoggerContext(
                name=name,
            )

            await logger._contexts[name].stream.initialize()

        logger._contexts[name].stream._provider.subscribe(self._contexts[name].stream._consumer)

    async def log(
        self,
        entry: T,
        name: str | None = None,
        template: str | None = None,
        path: str | None = None,
        retention_policy: RetentionPolicyConfig | None = None,
        filter: Callable[[T], bool] | None = None,
        models: dict[
            str,
            tuple[
                type[T],
                dict[str, Any],
            ]
        ] | None = None,
    ):
        if name is None:
            name = 'default'

        frame = sys._getframe(1)
        code = frame.f_code

        async with self.context(
            name=name,
            nested=True,
            models=models,
        ) as ctx:
            await ctx.log(
                Log(
                    entry=entry,
                    filename=code.co_filename,
                    function_name=code.co_name,
                    line_number=frame.f_lineno,
                    thread_id=threading.get_native_id(),
                    timestamp=datetime.datetime.now(datetime.UTC).isoformat()
                ),
                template=template,
                path=path,
                retention_policy=retention_policy,
                filter=filter,
            )

    async def batch(
        self,
        *entries: T | Log[T],
        name: str | None = None,
        models: dict[
            str,
            tuple[
                type[T],
                dict[str, Any],
            ]
        ] | None = None,
    ):
        if name is None:
            name = 'default'

        frame = sys._getframe(1)
        code = frame.f_code

        async with self.context(
            name=name,
            nested=True,
            models=models,
        ) as ctx:
            await asyncio.gather(*[
                ctx.put(
                    Log(
                        entry=entry,
                        filename=code.co_filename,
                        function_name=code.co_name,
                        line_number=frame.f_lineno,
                        thread_id=threading.get_native_id(),
                        timestamp=datetime.datetime.now(datetime.UTC).isoformat()
                    ),
                ) for entry in entries
            ])

    async def put(
        self,
        entry: T | Log[T],
        name: str | None = None,
        models: dict[
            str,
            tuple[
                type[T],
                dict[str, Any],
            ]
        ] | None = None,
    ):
        if name is None:
            name = 'default'

        frame = sys._getframe(1)
        code = frame.f_code
        
        async with self.context(
            name=name,
            nested=True,
            models=models,
        ) as ctx:
            await ctx.put(
                Log(
                    entry=entry,
                    filename=code.co_filename,
                    function_name=code.co_name,
                    line_number=frame.f_lineno,
                    thread_id=threading.get_native_id(),
                    timestamp=datetime.datetime.now(datetime.UTC).isoformat()
                ),
            )

    def watch(
        self, 
        name: str | None = None,
        filter: Callable[[T], bool] | None = None,
        models: dict[
            str,
            tuple[
                type[T],
                dict[str, Any],
            ]
        ] | None = None,
    ):

        if name is None:
            name = 'default'

        if self._watch_tasks.get(name):
            try:
                self._watch_tasks[name].cancel()

            except (
                asyncio.CancelledError,
                asyncio.InvalidStateError
            ):
                pass

        self._watch_tasks[name] = asyncio.create_task(
            self._watch(
                name,
                filter=filter,
                models=models,
            )
        )

    async def _watch(
        self, 
        name: str,
        filter: Callable[[T], bool] | None = None,
        models: dict[
            str,
            tuple[
                type[T],
                dict[str, Any],
            ]
        ] | None = None,
    ):
        async with self.context(
            name=name,
            nested=True,
            models=models,
        ) as ctx:
            async for log in ctx.get(
                filter=filter
            ):
                await ctx.log(log)

    async def stop_watch(
        self,
        name: str | None = None
    ):
        
        if name is None:
            name = 'default'
            
        if (
            context := self._contexts.get(name)
        ) and (
            watch_task := self._watch_tasks.get(name)
        ):
            await context.stream.close(shutdown_subscribed=True)
            
            try:
                await watch_task

            except (
                asyncio.CancelledError,
                asyncio.InvalidStateError,
            ):
                pass

    async def close(self):
    
        if len(self._watch_tasks) > 0:
            await asyncio.gather(*[
                self.stop_watch(name) for name in self._watch_tasks
            ])

        shutdown_subscribed = len([
            context for context in self._contexts.values() if context.stream.has_active_subscriptions
        ]) > 0

        contexts_count = len(self._contexts)

        if contexts_count > 0:
            await asyncio.gather(*[
                context.stream.close(
                    shutdown_subscribed=shutdown_subscribed
                ) for context in self._contexts.values()
            ])

    def abort(self):

        for context in self._contexts.values():
            context.stream.abort()
            



    