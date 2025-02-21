import asyncio
import ctypes
import functools
import multiprocessing
import signal
import warnings
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from multiprocessing.context import SpawnContext
from typing import Dict, List

from hyperscale.core.jobs.graphs.remote_graph_controller import (
    RemoteGraphController,
)
from hyperscale.core.jobs.models import Env
from hyperscale.logging import Entry, Logger, LoggingConfig, LogLevel, LogLevelName


def set_process_name():
    try:
        libc = ctypes.CDLL("libc.so.6")
        progname = ctypes.c_char_p.in_dll(
            libc, "__progname_full"
        )  # refer to the source code of glibc

        new_name = b"hyperscale"
        # for `ps` command:
        # Environment variables are already copied to the Python program zone.
        # We can get environment variables by using `os.environ`,
        # hence we can ignore both reallocation and movement.
        libc.strcpy(progname, ctypes.c_char_p(new_name))
        # for `top` command and `/proc/self/comm`:
        buff = ctypes.create_string_buffer(len(new_name) + 1)
        buff.value = new_name
        libc.prctl(15, ctypes.byref(buff), 0, 0, 0)

    except Exception:
        pass

    except OSError:
        pass


async def run_server(
    leader_address: tuple[str, int],
    server: RemoteGraphController,
    cert_path: str | None = None,
    key_path: str | None = None,
):
    try:
        await server.start_server(
            cert_path=cert_path,
            key_path=key_path,
        )

        try:
            await server.connect_client(leader_address)
            await server.acknowledge_start(leader_address)

        except Exception:
            pass
        
        await server.run_forever()
        await server.close()

    except (
        Exception,
        asyncio.CancelledError,
        KeyboardInterrupt,
        multiprocessing.ProcessError,
        OSError,
        asyncio.InvalidStateError,
        BrokenProcessPool,
        AssertionError,
    ):
        server.stop()
        await server.close()

    current_task = asyncio.current_task()

    tasks = asyncio.all_tasks()
    for task in tasks:
        if task != current_task:
            try:
                task.cancel()

            except (
                Exception,
                asyncio.InvalidStateError,
                asyncio.CancelledError,
                asyncio.TimeoutError,
                AssertionError,
            ):
                pass

    try:
        await asyncio.gather(
            *[task for task in tasks if task != current_task], return_exceptions=True
        )

    except Exception:
        pass


def run_thread(
    worker_idx: int,
    leader_address: tuple[str, int],
    worker_ip: tuple[str, int],
    worker_env: Dict[str, str | int | float | bool | None],
    logs_directory: str,
    log_level: LogLevelName = "info",
    cert_path: str | None = None,
    key_path: str | None = None,
):
    try:
        from hyperscale.logging import LoggingConfig

        try:
            import uvloop

            uvloop.install()

        except ImportError:
            pass

        import asyncio
        import logging

        logging.disable(logging.CRITICAL)

        logging_config = LoggingConfig()
        logging_config.update(
            log_directory=logs_directory,
            log_level=log_level,
            log_output="stderr",
        )

        try:
            loop = asyncio.get_event_loop()
        except Exception:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        host, port = worker_ip

        env = Env(**worker_env)

        server = RemoteGraphController(
            worker_idx + 1,
            host,
            port,
            env,
        )

        loop.run_until_complete(
            run_server(
                leader_address,
                server,
                cert_path=cert_path,
                key_path=key_path,
            )
        )

    except (
        Exception,
        OSError,
        multiprocessing.ProcessError,
        asyncio.CancelledError,
        asyncio.InvalidStateError,
    ):
        pass


class LocalServerPool:
    def __init__(
        self,
        pool_size: int,
    ) -> None:
        self._pool_size = pool_size
        self._context: SpawnContext | None = None
        self._executor: ProcessPoolExecutor | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._pool_task: asyncio.Task | None = None
        self._run_future: asyncio.Future | None = None
        self._logger = Logger()

    async def setup(self):
        self._context = multiprocessing.get_context("spawn")
        self._executor = ProcessPoolExecutor(
            max_workers=self._pool_size,
            mp_context=self._context,
            initializer=set_process_name,
            max_tasks_per_child=1,
        )

        async with self._logger.context(
            name="local_server_pool",
            path="hyperscale.leader.log.json",
            template="{timestamp} - {level} - {thread_id} - {filename}:{function_name}.{line_number} - {message}",
        ) as ctx:
            await ctx.log(
                Entry(
                    message="Creating interrupt handlers for local server pool",
                    level=LogLevel.TRACE,
                )
            )

            self._loop = asyncio.get_event_loop()

            for signame in ("SIGINT", "SIGTERM", "SIG_IGN"):
                self._loop.add_signal_handler(
                    getattr(
                        signal,
                        signame,
                    ),
                    self.abort,
                )

            await ctx.log(
                Entry(
                    message="Created interrupt handlers for local server pool",
                    level=LogLevel.TRACE,
                )
            )

    async def run_pool(
        self,
        leader_address: tuple[str, int],
        worker_ips: List[tuple[str, int]],
        env: Env,
        cert_path: str | None = None,
        key_path: str | None = None,
    ):
        async with self._logger.context(
            name="local_server_pool",
            path="hyperscale.leader.log.json",
            template="{timestamp} - {level} - {thread_id} - {filename}:{function_name}.{line_number} - {message}",
        ) as ctx:
            try:
                leader_host, leader_port = leader_address

                await ctx.log(
                    Entry(
                        message=f"Creating server pool with {self._pool_size} workers and leader at {leader_host}:{leader_port}",
                        level=LogLevel.DEBUG,
                    )
                )

                config = LoggingConfig()

                self._pool_task = asyncio.gather(
                    *[
                        self._loop.run_in_executor(
                            self._executor,
                            functools.partial(
                                run_thread,
                                idx,
                                leader_address,
                                worker_ip,
                                env.model_dump(),
                                config.directory,
                                log_level=config.level.name.lower(),
                                cert_path=cert_path,
                                key_path=key_path,
                            ),
                        )
                        for idx, worker_ip in enumerate(worker_ips)
                    ],
                    return_exceptions=True,
                )

            except (Exception, KeyboardInterrupt):
                pass

    async def shutdown(self, wait: bool = True):
        async with self._logger.context(
            name="local_server_pool",
            path="hyperscale.leader.log.json",
            template="{timestamp} - {level} - {thread_id} - {filename}:{function_name}.{line_number} - {message}",
        ) as ctx:
            await ctx.log(
                Entry(
                    message="Server pool received shutdown request",
                    level=LogLevel.DEBUG,
                )
            )

            try:
                if self._pool_task:
                    self._pool_task.set_result(None)

            except Exception:
                pass

            except asyncio.CancelledError:
                pass

            except asyncio.InvalidStateError:
                pass

            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")

                    if self._executor._processes and len(self._executor._processes) > 0:
                        await self._loop.run_in_executor(
                            None,
                            functools.partial(
                                self._executor.shutdown,
                                wait=False,
                                cancel_futures=True,
                            ),
                        )

            except (
                Exception,
                KeyboardInterrupt,
                asyncio.CancelledError,
                asyncio.InvalidStateError,
            ):
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")

                    if self._executor._processes and len(self._executor._processes) > 0:
                        await self._loop.run_in_executor(
                            None,
                            functools.partial(
                                self._executor.shutdown,
                                wait=False,
                                cancel_futures=True,
                            ),
                        )

            await ctx.log(
                Entry(
                    message="Server pool successfully shutdown",
                    level=LogLevel.DEBUG,
                )
            )

    def abort(self):
        try:
            if self._pool_task:
                self._pool_task.set_result(None)

        except Exception:
            pass

        except asyncio.CancelledError:
            pass

        except asyncio.InvalidStateError:
            pass

        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")

                if self._executor._processes and len(self._executor._processes) > 0:
                    self._executor.shutdown(wait=False, cancel_futures=True)

        except Exception:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")

                if self._executor._processes and len(self._executor._processes) > 0:
                    self._executor.shutdown(wait=False, cancel_futures=True)
