import asyncio
import atexit
import ctypes
import functools
import multiprocessing
import signal
import warnings
import weakref
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from multiprocessing.context import SpawnContext
from typing import Callable, Dict, List

from hyperscale.core.runtime import ProcessSpawner, SimulationChildContext


# Module-level weak reference set for atexit cleanup
_active_pools: weakref.WeakSet["LocalServerPool"] = weakref.WeakSet()


def _atexit_cleanup():
    """Cleanup any remaining pools on interpreter exit."""
    for pool in list(_active_pools):
        try:
            pool.abort()
        except Exception:
            pass


atexit.register(_atexit_cleanup)

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
    enable_server_cleanup: bool = False,
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
            server.stop()
            await server.close()

            return

        if enable_server_cleanup:
            server.start_controller_cleanup()

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

    # Wait for tasks with a timeout to prevent hanging
    try:
        pending_tasks = [task for task in tasks if task != current_task]
        if pending_tasks:
            # Use asyncio.wait instead of gather+wait_for for better control
            done, still_pending = await asyncio.wait(
                pending_tasks,
                timeout=5.0,
                return_when=asyncio.ALL_COMPLETED,
            )

            # Force cancel any tasks that didn't complete in time
            for task in still_pending:
                task.cancel()

            # Wait briefly for cancellation to propagate
            if still_pending:
                await asyncio.wait(still_pending, timeout=1.0)

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
    enable_server_cleanup: bool = False,
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
                enable_server_cleanup=enable_server_cleanup,
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


def run_sim_executor(
    context: SimulationChildContext,
    worker_index: int,
    leader_address: tuple[str, int],
    worker_address: tuple[str, int],
    worker_env: Dict[str, str | int | float | bool | None],
    cert_path: str | None,
    key_path: str | None,
    enable_server_cleanup: bool,
) -> None:
    """SIM counterpart of ``run_thread`` — one pool executor as a
    coordinator child process.

    Runs inside a freshly spawned simulation child: builds the same
    ``RemoteGraphController`` ``run_thread`` builds, but pinned to the
    child's ``SimulationLoop`` and cross-process transport, then drives
    the *identical* production lifecycle (``run_server``: start, connect
    back to the leader, acknowledge, serve until stopped). No uvloop, no
    logging reconfiguration, no fresh event loop — the child runtime owns
    all three. Top-level so ``spawn`` can re-import it by module +
    qualname in the executor process.
    """
    worker_host, worker_port = worker_address
    server = RemoteGraphController(
        worker_index + 1,
        worker_host,
        worker_port,
        Env(**worker_env),
        loop=context.loop,
        transport_factory=context.transport,
    )

    context.loop.create_task(
        run_server(
            leader_address,
            server,
            cert_path=cert_path,
            key_path=key_path,
            enable_server_cleanup=enable_server_cleanup,
        )
    )


class LocalServerPool:
    def __init__(
        self,
        pool_size: int,
        *,
        loop: asyncio.AbstractEventLoop | None = None,
        process_spawner: ProcessSpawner | None = None,
        on_executor_exit: Callable[[tuple[str, int]], None] | None = None,
    ) -> None:
        # Phase 6 SIM seam. ``process_spawner`` is ``None`` in REAL mode —
        # the pool fans its executors out through a ``ProcessPoolExecutor``
        # exactly as before. Under SIM the spawner (the coordinator child
        # context) requests each executor as a *coordinator child process*
        # instead, so every executor still runs in its own OS process
        # (multi-process preserved) but on a ``SimulationLoop`` the
        # ``SimulationCoordinator`` keeps in lockstep. The coordinator
        # then owns executor lifecycle end-to-end — ``shutdown`` /
        # ``abort`` have no subprocesses of their own to reap. ``loop``
        # pins the pool to the caller's ``SimulationLoop`` so lazy
        # ``get_event_loop`` never resolves a non-simulation loop.
        self._pool_size = pool_size
        self._context: SpawnContext | None = None
        self._executor: ProcessPoolExecutor | None = None
        self._loop: asyncio.AbstractEventLoop | None = loop
        self._process_spawner = process_spawner
        # Told the listen address of every executor whose process exit
        # the pool reaps, at the reap — the pool leader withdraws that
        # executor from its hand-outs before anything can dispatch to it.
        self._on_executor_exit = on_executor_exit
        # SIM: live executor process id -> (worker index, worker address)
        # of the slot it fills; a reaped id leaves, its replacement joins.
        self._executor_slots: dict[str, tuple[int, tuple[str, int]]] = {}
        # SIM: replacements spawned per slot address, so every replacement
        # gets a process id no earlier generation used.
        self._executor_respawn_counts: dict[tuple[str, int], int] = {}
        # SIM: (leader address, worker env, cert path, key path, server
        # cleanup) every executor of this pool is spawned with.
        self._executor_spawn_arguments: tuple | None = None
        self._pool_task: asyncio.Task | None = None
        self._run_future: asyncio.Future | None = None
        self._logger = Logger()
        self._cleaned_up = False

        # Register for atexit cleanup
        _active_pools.add(self)

    async def setup(self):
        if self._process_spawner is not None:
            # SIM: executors are spawned as coordinator child processes at
            # ``run_pool`` time — no ``ProcessPoolExecutor``, and signal
            # handlers are banned on the ``SimulationLoop`` (the
            # coordinator, not signals, drives shutdown).
            if self._loop is None:
                self._loop = asyncio.get_event_loop()
            self._process_spawner.set_process_exit_listener(
                self._handle_simulation_executor_exit
            )
            return

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

            # Handle SIGINT, SIGTERM, and SIGHUP
            for signame in ("SIGINT", "SIGTERM", "SIGHUP"):
                try:
                    self._loop.add_signal_handler(
                        getattr(signal, signame),
                        self.abort,
                    )
                except (ValueError, OSError):
                    # Signal not available on this platform
                    pass

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
        enable_server_cleanup: bool = False,
    ):
        if self._process_spawner is not None:
            self._spawn_simulation_executors(
                leader_address,
                worker_ips,
                env,
                cert_path=cert_path,
                key_path=key_path,
                enable_server_cleanup=enable_server_cleanup,
            )
            return

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

                # Ctrl+C belongs to the leader, which shuts the workers down.
                # Spawned with SIGINT blocked, the workers (and the executor
                # thread that later replaces them) inherit the mask through
                # fork and exec, so the terminal's SIGINT never interrupts their
                # imports or their run. A SIGINT that arrives meanwhile stays
                # pending and reaches the leader once unblocked.
                leader_signal_mask = (
                    signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})
                    if hasattr(signal, "pthread_sigmask")
                    else None
                )

                try:
                    executor_futures = [
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
                                enable_server_cleanup=enable_server_cleanup,
                            ),
                        )
                        for idx, worker_ip in enumerate(worker_ips)
                    ]

                    # An executor's future completes when the pool reaps its
                    # process — on return, or with ``BrokenProcessPool`` when
                    # it died abruptly. That completion is the reap event.
                    # No respawn here: an abrupt death breaks the
                    # ``ProcessPoolExecutor`` (it terminates every sibling and
                    # refuses new work), so each sibling is reaped in turn.
                    self._watch_executor_futures(executor_futures, worker_ips)

                    self._pool_task = asyncio.gather(
                        *executor_futures,
                        return_exceptions=True,
                    )

                finally:
                    if leader_signal_mask is not None:
                        signal.pthread_sigmask(signal.SIG_SETMASK, leader_signal_mask)

            except (Exception, KeyboardInterrupt):
                pass

    def _spawn_simulation_executors(
        self,
        leader_address: tuple[str, int],
        worker_ips: List[tuple[str, int]],
        env: Env,
        *,
        cert_path: str | None,
        key_path: str | None,
        enable_server_cleanup: bool,
    ) -> None:
        """SIM counterpart of the ``ProcessPoolExecutor`` fan-out.

        Requests one coordinator child per worker address, each running
        ``run_sim_executor`` — the production executor lifecycle on a
        lockstep ``SimulationLoop``. Process ids are derived from the
        (globally unique) worker addresses, so two pools in one
        simulation can never collide. The children are admitted by the
        coordinator at the next window barrier, starting at the current
        global virtual time.
        """
        self._executor_spawn_arguments = (
            leader_address,
            env.model_dump(),
            cert_path,
            key_path,
            enable_server_cleanup,
        )
        for worker_index, worker_address in enumerate(worker_ips):
            worker_host, worker_port = worker_address
            self._spawn_simulation_executor(
                f"executor-{worker_host}-{worker_port}",
                worker_index,
                worker_address,
            )

    def _spawn_simulation_executor(
        self,
        process_id: str,
        worker_index: int,
        worker_address: tuple[str, int],
    ) -> None:
        """Request one executor child for the slot at ``worker_address``.

        The single SIM spawn path: the initial fan-out and every
        replacement go through it, so a replacement runs the identical
        ``run_sim_executor`` lifecycle — including the start
        acknowledgement that is its ready handshake with the leader.
        """
        (
            leader_address,
            worker_env,
            cert_path,
            key_path,
            enable_server_cleanup,
        ) = self._executor_spawn_arguments
        self._executor_slots[process_id] = (worker_index, worker_address)
        self._process_spawner.spawn_process(
            process_id,
            run_sim_executor,
            worker_index,
            leader_address,
            worker_address,
            worker_env,
            cert_path,
            key_path,
            enable_server_cleanup,
        )

    def _handle_simulation_executor_exit(self, process_id: str, exitcode: int) -> None:
        """Reap listener: withdraw the dead executor, then refill its slot.

        Runs at the instant the spawner reaps ``process_id``. The leader is
        told first, so the slot is out of every hand-out before the
        replacement is even requested; the replacement takes the slot back
        only through its own start acknowledgement. A pool that is shutting
        down does not respawn.
        """
        worker_index, worker_address = self._executor_slots.pop(process_id)
        if self._on_executor_exit is not None:
            self._on_executor_exit(worker_address)

        if self._cleaned_up:
            return

        respawn_count = self._executor_respawn_counts.get(worker_address, 0) + 1
        self._executor_respawn_counts[worker_address] = respawn_count
        worker_host, worker_port = worker_address
        self._spawn_simulation_executor(
            f"executor-{worker_host}-{worker_port}-respawn-{respawn_count}",
            worker_index,
            worker_address,
        )

    def _watch_executor_futures(
        self,
        executor_futures: List[asyncio.Future],
        worker_ips: List[tuple[str, int]],
    ) -> None:
        """REAL: report each executor's exit when the pool reaps it."""
        if self._on_executor_exit is None:
            return

        for executor_future, worker_address in zip(executor_futures, worker_ips):
            executor_future.add_done_callback(
                functools.partial(self._report_executor_future_exit, worker_address)
            )

    def _report_executor_future_exit(
        self,
        worker_address: tuple[str, int],
        executor_future: asyncio.Future,
    ) -> None:
        """REAL reap listener: the executor at ``worker_address`` exited."""
        self._on_executor_exit(worker_address)

    def get_process_exitcodes(self) -> dict[int | str, int | None]:
        """Return a snapshot of worker-process id to exit code.

        ``None`` means the process is still running. A non-``None`` exit code
        means the process exited and any workflow assigned to that local
        controller is no longer making progress. Keys are OS pids in REAL
        mode and coordinator process ids (strings) under SIM — consumers
        treat them opaquely. Under SIM the snapshot comes from the
        spawner seam, where fault-injected kills surface at their exact
        virtual instant.
        """
        if self._process_spawner is not None:
            return self._process_spawner.get_process_exitcodes()

        if self._executor is None:
            return {}

        processes = getattr(self._executor, "_processes", None)
        if not processes:
            return {}

        return {
            int(process_id): process.exitcode
            for process_id, process in list(processes.items())
        }

    async def shutdown(self, wait: bool = True):
        # Prevent double cleanup
        if self._cleaned_up:
            return
        self._cleaned_up = True

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

            # Cancel the pool task first
            try:
                if self._pool_task and not self._pool_task.done():
                    self._pool_task.cancel()
                    try:
                        await asyncio.wait_for(self._pool_task, timeout=0.25)
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        pass

            except (Exception, asyncio.CancelledError, asyncio.InvalidStateError):
                pass

            # Shutdown executor - do NOT use the executor to shut itself down
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")

                    if self._executor and self._executor._processes:
                        # Kill processes immediately - no graceful termination needed
                        for pid, proc in list(self._executor._processes.items()):
                            if proc.is_alive():
                                try:
                                    proc.kill()
                                except Exception:
                                    pass

                        # Now shutdown the executor (processes are already dead)
                        self._executor.shutdown(wait=False, cancel_futures=True)

                    # Clear executor reference to allow GC
                    self._executor = None

            except (
                Exception,
                KeyboardInterrupt,
                asyncio.CancelledError,
                asyncio.InvalidStateError,
            ):
                # Last resort: force shutdown without wait
                try:
                    if self._executor:
                        self._executor.shutdown(wait=False, cancel_futures=True)
                        self._executor = None
                except Exception:
                    pass

            # Remove from active pools set
            _active_pools.discard(self)

            await ctx.log(
                Entry(
                    message="Server pool successfully shutdown",
                    level=LogLevel.DEBUG,
                )
            )

    def abort(self):
        # Prevent double cleanup
        if self._cleaned_up:
            return
        self._cleaned_up = True

        try:
            if self._pool_task and not self._pool_task.done():
                self._pool_task.cancel()
        except (Exception, asyncio.CancelledError, asyncio.InvalidStateError):
            pass

        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")

                if self._executor and self._executor._processes:
                    # Force kill all processes immediately
                    for pid, proc in list(self._executor._processes.items()):
                        try:
                            if proc.is_alive():
                                proc.kill()
                        except Exception:
                            pass

                    # Shutdown executor
                    self._executor.shutdown(wait=False, cancel_futures=True)

                # Clear executor reference to allow GC
                self._executor = None

        except Exception:
            pass

        # Remove from active pools set
        _active_pools.discard(self)
