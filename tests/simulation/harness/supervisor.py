"""
Supervisor — owns the lifetime and cleanup of every artifact the harness
creates: server handles, worker subprocess PIDs, ports, asyncio tasks
the harness itself spawned.

The cleanup contract:

* `__aexit__` never raises. Every error is collected into the supervisor's
  `cleanup_errors` list and surfaced through the harness on test failure.
* Reaping is layered: graceful → forced → SIGKILL → final descendant sweep.
  Each layer has its own timeout so a hung component degrades gracefully.
* Asyncio task leaks are detected by diffing `asyncio.all_tasks()` against
  the supervisor's own baseline.

See docs/dev/simulation_framework.md §6 and §7.
"""

import asyncio
import os
import time
import uuid
from dataclasses import dataclass, field

import psutil

from tests.simulation.harness.errors import (
    LeakedAsyncTasksError,
    PreflightZombieError,
    ReapError,
)
from tests.simulation.harness.port_allocator import PortAllocator
from tests.simulation.harness.server_handle import ServerHandle, ServerKind
from tests.simulation.harness.timeouts import HarnessTimeouts


_HARNESS_RUN_ID_ENV = "HYPERSCALE_HARNESS_RUN_ID"


@dataclass(slots=True)
class Supervisor:
    """Centralized lifetime + cleanup of every artifact the harness owns.

    Construction does not allocate anything. `__aenter__` runs preflight
    (zombie reap, baseline snapshot). Servers are registered as the
    `ClusterHarness` builds them. `__aexit__` reaps everything and surfaces
    a `cleanup_errors` report; the harness raises on a leak unless the
    caller opted out.
    """

    timeouts: HarnessTimeouts
    ports: PortAllocator
    fail_on_async_leak: bool = True
    """Per design §19 open question: lean fail-immediately."""

    _run_id: str = field(init=False, default="")
    _server_handles: list[ServerHandle] = field(init=False, default_factory=list)
    _tracked_pids: dict[str, set[int]] = field(init=False, default_factory=dict)
    _pid_track_tasks: list[asyncio.Task] = field(init=False, default_factory=list)
    _baseline_pids: set[int] = field(init=False, default_factory=set)
    _baseline_tasks: set[asyncio.Task] = field(init=False, default_factory=set)
    _running: bool = field(init=False, default=False)
    cleanup_errors: list[str] = field(init=False, default_factory=list)

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def server_handles(self) -> list[ServerHandle]:
        return list(self._server_handles)

    def tracked_pids(self, node_id: str) -> set[int]:
        return set(self._tracked_pids.get(node_id, set()))

    async def __aenter__(self) -> "Supervisor":
        self._run_id = uuid.uuid4().hex
        os.environ[_HARNESS_RUN_ID_ENV] = self._run_id

        await self._preflight_zombie_reap()

        self._baseline_pids = {
            proc.pid for proc in psutil.Process().children(recursive=True)
        }
        self._baseline_tasks = set(asyncio.all_tasks())
        self._running = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.shutdown()

    def register_server(self, handle: ServerHandle) -> None:
        """Add a server handle. Call after construction, before `start()`.

        For workers, call `start_worker_pid_tracking` once `start()` has
        returned so the PID tick task has something to snapshot.
        """
        self._server_handles.append(handle)
        if handle.kind is ServerKind.WORKER:
            self._tracked_pids[handle.node_id] = set()

    def start_worker_pid_tracking(self, handle: ServerHandle) -> None:
        """Begin the 1 s tick that snapshots a worker's subprocess PIDs.

        Reaches into `_lifecycle_manager._server_pool._executor._processes`
        — same private API the existing `kill_child_processes()` uses.
        Degrades gracefully if the structure is missing: the `psutil`
        baseline-diff in `_final_descendant_sweep` remains the source of
        truth for cleanup correctness.
        """
        if handle.kind is not ServerKind.WORKER:
            return
        task = asyncio.create_task(
            self._tick_worker_pids(handle),
            name=f"sim-pid-track-{handle.node_id}",
        )
        self._pid_track_tasks.append(task)

    async def shutdown(self) -> None:
        """Quiescence-driven cluster teardown.

        The naive approach — call ``server.stop()`` per node with a fixed
        ``wait_for`` budget — is brittle: if the budget fires, cancellation
        cascades into mid-flight cleanup and leaves partial state. The
        approach below is convergence-driven instead:

        1. **Signal** every server to stop. Fire `server.stop()` calls in
           parallel without imposing a wait_for budget. Servers stop
           accepting new work and begin draining.
        2. **Quiescence** — poll `asyncio.all_tasks()` until the count of
           harness-spawned tasks is stable (or strictly decreasing) for
           ``quiescence_stable_ticks`` consecutive ticks. This means the
           system has stopped scheduling new work and existing tasks are
           winding down.
        3. **Force-cancel** anything that survived quiescence. By
           definition these tasks are not draining on their own. Cancel
           in parallel, await briefly. Whatever still survives ignored
           cancellation — that's a real leak.
        4. **Subprocess sweep + port verify + leak report** as before.

        The key distinction: quiescence is a *property of the system* we
        wait for, not a *budget we cross our fingers on*. Healthy
        teardowns settle in tens of ms; pathological ones surface
        diagnosable behaviour.
        """
        if not self._running:
            return
        self._running = False

        # Stop PID-tracking tasks first so the per-server reaps below own the
        # final pid snapshot without races.
        await self._stop_pid_tracking()

        await self._signal_servers_stop()
        await self._await_task_quiescence()
        truly_leaked = await self._force_cancel_survivors()

        await self._reap_worker_subprocess_pools()
        await self._final_descendant_sweep()
        await self._verify_ports_released()
        self._report_async_leaks(truly_leaked)

        os.environ.pop(_HARNESS_RUN_ID_ENV, None)

    async def _signal_servers_stop(self) -> None:
        """Phase 1: fire ``server.stop()`` for every started node in parallel.

        Workers first (they hold subprocesses), then managers, then gates —
        same dependency order as before, but now without per-node
        ``wait_for`` budgets. We expect each ``stop`` call to return; if
        one hangs, the quiescence phase still bounds total time, and the
        force-cancel phase reaps anything pinned.
        """
        for kind in (ServerKind.WORKER, ServerKind.MANAGER, ServerKind.GATE):
            handles = [
                h for h in self._server_handles if h.kind is kind and h.started
            ]
            if not handles:
                continue
            await asyncio.gather(
                *(self._signal_server_stop(h) for h in handles),
                return_exceptions=True,
            )

    async def _signal_server_stop(self, handle: ServerHandle) -> None:
        """Invoke ``stop`` once for a single handle. Errors recorded, not raised.

        ``drain_timeout=0`` because the harness tear-down does not need to
        wait for in-flight messages — quiescence handles that holistically.
        """
        try:
            await handle.instance.stop(drain_timeout=0.0, broadcast_leave=False)
        except Exception as stop_error:
            self.cleanup_errors.append(
                f"server.stop {handle.node_id}: "
                f"{type(stop_error).__name__}: {stop_error}"
            )

    async def _await_task_quiescence(self) -> None:
        """Phase 2: wait until harness-spawned task count stops growing.

        Runs at ``quiescence_poll_interval``. Tracks the count of tasks
        spawned during the cluster's lifetime that aren't yet ``done()``.
        Declares quiescence when that count has been stable (or
        strictly decreasing) for ``quiescence_stable_ticks`` consecutive
        polls. Hard-bounded by ``quiescence_max_seconds`` so a runaway
        production loop can't pin teardown forever.
        """
        deadline = time.monotonic() + self.timeouts.quiescence_max_seconds
        last_count = -1
        stable_ticks = 0
        while time.monotonic() < deadline:
            await asyncio.sleep(self.timeouts.quiescence_poll_interval)
            current = self._count_harness_unfinished_tasks()
            if last_count >= 0 and current <= last_count:
                stable_ticks += 1
                if stable_ticks >= self.timeouts.quiescence_stable_ticks:
                    return
            else:
                stable_ticks = 0
            last_count = current

        self.cleanup_errors.append(
            "quiescence not reached within "
            f"{self.timeouts.quiescence_max_seconds}s; surviving tasks "
            "will be reported by the leak detector"
        )

    async def _force_cancel_survivors(self) -> list[asyncio.Task]:
        """Phase 3: cancel tasks that survived quiescence.

        These tasks did not wind down naturally during quiescence — they
        are leaked. Per Python's asyncio contract, when a coroutine
        catches ``CancelledError``, the cancellation is *consumed* — a
        single ``task.cancel()`` is not enough for cooperatively-bad
        loops like ``while self._running: try: await ...; except
        CancelledError: pass`` (which the framework uses in several
        places). The fix is **persistent cancellation**: cancel,
        wait briefly, observe what survived, cancel again, repeat —
        until either the task ends or we exhaust the budget.

        Each round uses ``asyncio.wait`` (not ``wait_for(gather)``) so
        the supervisor's own coroutine doesn't get cascaded into.
        """
        survivors = self._collect_harness_unfinished_tasks()
        if not survivors:
            return []

        deadline = time.monotonic() + self.timeouts.force_cancel_settle_seconds
        outstanding = list(survivors)
        per_round_budget = max(0.05, self.timeouts.force_cancel_round_seconds)

        while outstanding and time.monotonic() < deadline:
            for task in outstanding:
                if not task.done():
                    task.cancel()
            _done, pending = await asyncio.wait(
                outstanding, timeout=per_round_budget
            )
            outstanding = [t for t in pending if not t.done()]

        return outstanding

    async def _reap_worker_subprocess_pools(self) -> None:
        """Reap worker subprocess pools after async cleanup is settled.

        The signal-and-quiesce flow above handles asyncio cleanup; this
        layer handles the OS subprocess pools the workers spawn via
        ``ProcessPoolExecutor``. Done after quiescence so the worker has
        already had the chance to shut its pool down cooperatively;
        anything left here is an OS-level orphan.
        """
        for handle in self._server_handles:
            if handle.kind is not ServerKind.WORKER:
                continue
            await self._reap_worker_subprocesses(handle, graceful_ok=True)

    def _count_harness_unfinished_tasks(self) -> int:
        return len(self._collect_harness_unfinished_tasks())

    def _collect_harness_unfinished_tasks(self) -> list[asyncio.Task]:
        try:
            current = asyncio.current_task()
        except RuntimeError:
            current = None
        result: list[asyncio.Task] = []
        for task in asyncio.all_tasks():
            if task in self._baseline_tasks:
                continue
            if task in self._pid_track_tasks:
                continue
            if task is current:
                continue
            if task.done():
                continue
            result.append(task)
        return result

    def _report_async_leaks(self, leaked: list[asyncio.Task]) -> None:
        if not leaked:
            return
        descriptions = sorted(_describe_leaked_task(task) for task in leaked)
        message = (
            f"{len(leaked)} async tasks survived cancellation:\n  - "
            + "\n  - ".join(descriptions)
        )
        self.cleanup_errors.append(message)
        if self.fail_on_async_leak:
            raise LeakedAsyncTasksError(message)

    async def _stop_pid_tracking(self) -> None:
        for task in self._pid_track_tasks:
            if not task.done():
                task.cancel()
        if self._pid_track_tasks:
            await asyncio.gather(*self._pid_track_tasks, return_exceptions=True)
        self._pid_track_tasks.clear()

    async def _tick_worker_pids(self, handle: ServerHandle) -> None:
        while self._running:
            try:
                self._tracked_pids[handle.node_id] = self._snapshot_worker_pids(handle)
            except Exception as snapshot_error:
                self.cleanup_errors.append(
                    f"pid-snapshot {handle.node_id}: {type(snapshot_error).__name__}: {snapshot_error}"
                )
            try:
                await asyncio.sleep(self.timeouts.pid_track_interval)
            except asyncio.CancelledError:
                break

    @staticmethod
    def _snapshot_worker_pids(handle: ServerHandle) -> set[int]:
        lifecycle = getattr(handle.instance, "_lifecycle_manager", None)
        if lifecycle is None:
            return set()
        pool = getattr(lifecycle, "_server_pool", None)
        if pool is None:
            return set()
        executor = getattr(pool, "_executor", None)
        if executor is None:
            return set()
        processes = getattr(executor, "_processes", None)
        if processes is None:
            return set()
        return set(processes.keys())

    async def _reap_worker_subprocesses(
        self, handle: ServerHandle, graceful_ok: bool
    ) -> None:
        """OS-level cleanup for the worker subprocess pool.

        Run after the asyncio quiescence pass: anything still alive in
        the worker's subprocess pool is an OS-level orphan, not an
        async task. Terminate, wait, kill, wait — same escalation as
        before. ``graceful_ok=True`` is passed by the new flow because
        we have already given the worker its chance to wind its pool
        down through ``server.stop()``.
        """
        if not graceful_ok:
            await self._invoke_lifecycle_kill(handle)

        pids = list(self._tracked_pids.get(handle.node_id, set()))
        procs = self._existing_processes(pids)
        if not procs:
            return

        for proc in procs:
            try:
                proc.terminate()
            except psutil.NoSuchProcess:
                continue
            except Exception as term_error:
                self.cleanup_errors.append(
                    f"SIGTERM {handle.node_id} pid={proc.pid}: "
                    f"{type(term_error).__name__}: {term_error}"
                )

        _gone, alive = psutil.wait_procs(procs, timeout=3.0)
        for proc in alive:
            try:
                proc.kill()
            except psutil.NoSuchProcess:
                continue
            except Exception as kill_error:
                self.cleanup_errors.append(
                    f"SIGKILL {handle.node_id} pid={proc.pid}: "
                    f"{type(kill_error).__name__}: {kill_error}"
                )

        # Final wait so the descendant sweep does not race with kernel reaping.
        psutil.wait_procs([psutil.Process(p.pid) for p in alive if psutil.pid_exists(p.pid)], timeout=2.0)

    async def _invoke_lifecycle_kill(self, handle: ServerHandle) -> None:
        lifecycle = getattr(handle.instance, "_lifecycle_manager", None)
        if lifecycle is None:
            return
        kill_child_processes = getattr(lifecycle, "kill_child_processes", None)
        if kill_child_processes is None:
            return
        try:
            await asyncio.wait_for(kill_child_processes(), timeout=3.0)
        except Exception as kill_error:
            self.cleanup_errors.append(
                f"lifecycle.kill_child_processes {handle.node_id}: "
                f"{type(kill_error).__name__}: {kill_error}"
            )

    async def _final_descendant_sweep(self) -> None:
        """The safety net: kill anything still hanging off our PID.

        Layered: SIGTERM → wait → SIGKILL → wait again, with a final
        zombie-tolerant pass. A zombie process (state == "zombie") has
        already exited; the kernel keeps the PID slot until the parent
        calls `wait()`. Zombies count as reaped for our purposes — the
        multiprocessing resource tracker will clean them up at exit.
        """
        try:
            current = {
                proc.pid for proc in psutil.Process().children(recursive=True)
            }
        except psutil.Error as walk_error:
            self.cleanup_errors.append(
                f"descendant-walk: {type(walk_error).__name__}: {walk_error}"
            )
            return

        leftover_pids = current - self._baseline_pids
        if not leftover_pids:
            return

        leftover_procs = self._existing_processes(list(leftover_pids))
        for proc in leftover_procs:
            try:
                proc.terminate()
            except psutil.NoSuchProcess:
                continue
            except Exception as term_error:
                self.cleanup_errors.append(
                    f"sweep SIGTERM pid={proc.pid}: "
                    f"{type(term_error).__name__}: {term_error}"
                )

        _gone, alive = psutil.wait_procs(leftover_procs, timeout=3.0)
        for proc in alive:
            try:
                proc.kill()
            except psutil.NoSuchProcess:
                continue
            except Exception as kill_error:
                self.cleanup_errors.append(
                    f"sweep SIGKILL pid={proc.pid}: "
                    f"{type(kill_error).__name__}: {kill_error}"
                )

        # Give SIGKILL more time, then accept zombies as reaped.
        _gone2, still_alive = psutil.wait_procs(alive, timeout=5.0)
        truly_alive = [p for p in still_alive if not _is_zombie(p)]

        if truly_alive:
            self.cleanup_errors.append(
                f"sweep left {len(truly_alive)} undeath-able processes: "
                f"{[p.pid for p in truly_alive]}"
            )

    async def _verify_ports_released(self) -> None:
        held = await self.ports.verify_all_released()
        if held:
            self.cleanup_errors.append(f"ports still held after teardown: {held}")

    async def _preflight_zombie_reap(self) -> None:
        """Find and kill processes left over from earlier harness runs.

        We tag every harness-spawned process by setting `HYPERSCALE_HARNESS_RUN_ID`
        in the env. Any process carrying that env var with a value *other than*
        our current run id is from a previous run that didn't clean up.

        Also asserts that the planned port range is not currently held by
        non-harness processes (would surface a real conflict, e.g. another
        local server using the same range).
        """
        loop = asyncio.get_running_loop()
        zombies = await loop.run_in_executor(None, self._scan_zombie_processes)
        if not zombies:
            return

        for proc in zombies:
            try:
                proc.terminate()
            except psutil.NoSuchProcess:
                continue
            except Exception:
                continue

        _gone, alive = await loop.run_in_executor(
            None, psutil.wait_procs, zombies, 3.0
        )
        for proc in alive:
            try:
                proc.kill()
            except psutil.NoSuchProcess:
                continue
            except Exception:
                continue

        await asyncio.sleep(0.5)

        still_alive = [p for p in alive if psutil.pid_exists(p.pid)]
        if still_alive:
            raise PreflightZombieError(
                f"Could not reap {len(still_alive)} prior-run zombies: "
                f"{[p.pid for p in still_alive]}"
            )

    def _scan_zombie_processes(self) -> list[psutil.Process]:
        """Walk all processes; return those carrying a foreign run id."""
        zombies: list[psutil.Process] = []
        for proc in psutil.process_iter(["environ"]):
            try:
                env = proc.info.get("environ") or {}
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
            if env is None:
                continue
            foreign_id = env.get(_HARNESS_RUN_ID_ENV)
            if not foreign_id:
                continue
            if foreign_id == self._run_id:
                continue
            zombies.append(proc)
        return zombies

    @staticmethod
    def _existing_processes(pids: list[int]) -> list[psutil.Process]:
        result: list[psutil.Process] = []
        for pid in pids:
            try:
                result.append(psutil.Process(pid))
            except psutil.NoSuchProcess:
                continue
        return result

    @staticmethod
    def _now() -> float:
        return time.monotonic()


def _describe_leaked_task(task: asyncio.Task) -> str:
    """Render a leaked task as ``name | qualname | file:line | awaiting``.

    The default ``task.get_name()`` returns "Task-N" for unnamed tasks,
    which is useless for finding the leak source. This helper extracts:

    * task name (``task.get_name()``)
    * coroutine qualified name (``coro.cr_code.co_qualname``)
    * source location (``co_filename:co_firstlineno``, repo-relative)
    * current await frame, if the task is waiting on something
      (last frame of ``task.get_stack()``)
    """
    name = task.get_name()
    coro = task.get_coro()
    qualname = "?"
    location = "?"
    if coro is not None:
        code = getattr(coro, "cr_code", None) or getattr(coro, "gi_code", None)
        if code is not None:
            qualname = getattr(code, "co_qualname", code.co_name)
            filename = code.co_filename
            line = code.co_firstlineno
            try:
                from pathlib import Path as _Path

                filename = str(
                    _Path(filename).resolve().relative_to(_Path.cwd())
                )
            except (ValueError, OSError):
                pass
            location = f"{filename}:{line}"

    awaiting = ""
    try:
        stack = task.get_stack(limit=1)
        if stack:
            frame = stack[0]
            await_code = frame.f_code
            await_qual = getattr(await_code, "co_qualname", await_code.co_name)
            awaiting = f" @ {await_qual}:{frame.f_lineno}"
    except Exception:
        pass

    return f"{name} | {qualname} | {location}{awaiting}"


def _is_zombie(proc: psutil.Process) -> bool:
    """Return True if the process is in zombie state.

    A zombie has exited; the kernel keeps the PID slot until the parent
    waits on it. For cleanup purposes a zombie counts as dead — the
    multiprocessing resource tracker reaps these at process exit.
    """
    try:
        return proc.status() == psutil.STATUS_ZOMBIE
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return True
