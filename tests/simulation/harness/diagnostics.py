"""
Diagnostic dumper.

On any condition-wait timeout, expectation failure, or invariant
violation, the harness writes a human-readable snapshot of the cluster
state, the OS process tree we own, and the asyncio task census to
``tests/simulation/_artifacts/<scenario>/<timestamp>/``. The dump is
the difference between a useful CI failure and an inscrutable one.

Phase 2 ships the structural skeleton with the snapshotters that work
on Phase 1 data. The `FaultMatrix` snapshotter is a stub until Phase 3
introduces fault injection; the same applies to job-leadership state
which only exists once a workload is dispatched.
"""

import asyncio
import datetime
import json
import pathlib
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import psutil

from tests.simulation.harness.server_handle import ServerHandle, ServerKind

if TYPE_CHECKING:
    from tests.simulation.harness.cluster_harness import ClusterHarness


@dataclass(slots=True)
class DiagnosticDump:
    """Path bundle for a single dump invocation."""

    scenario: str
    artifacts_root: pathlib.Path
    written_files: list[pathlib.Path] = field(default_factory=list)


@dataclass(slots=True)
class DiagnosticDumper:
    """Snapshot the harness on demand.

    Construction does not touch the filesystem. Each ``dump`` call
    creates a fresh subdirectory under ``artifacts_root`` containing
    one JSON-Lines file per snapshot kind; subsequent dumps in the
    same scenario do not overwrite earlier ones.
    """

    artifacts_root: pathlib.Path
    scenario: str
    harness: "ClusterHarness"

    async def dump(self, reason: str) -> DiagnosticDump:
        """Write a complete snapshot. Safe to call from any failure path.

        Never raises. Failures during snapshot collection are written to
        the dump itself so the test report shows what we could not
        capture.
        """
        timestamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%S_%f")
        run_dir = self.artifacts_root / self.scenario / timestamp
        run_dir.mkdir(parents=True, exist_ok=True)
        result = DiagnosticDump(scenario=self.scenario, artifacts_root=run_dir)

        await self._write(
            result,
            run_dir / "00_reason.txt",
            text=reason,
        )
        await self._write_jsonl(
            result,
            run_dir / "01_cluster_topology.jsonl",
            self._topology_records(),
        )
        await self._write_jsonl(
            result,
            run_dir / "02_node_state.jsonl",
            self._node_state_records(),
        )
        await self._write_jsonl(
            result,
            run_dir / "03_process_tree.jsonl",
            self._process_tree_records(),
        )
        await self._write_jsonl(
            result,
            run_dir / "04_pid_attribution.jsonl",
            self._pid_attribution_records(),
        )
        await self._write_jsonl(
            result,
            run_dir / "05_asyncio_tasks.jsonl",
            self._asyncio_task_records(),
        )
        await self._write_jsonl(
            result,
            run_dir / "06_supervisor_errors.jsonl",
            self._supervisor_error_records(),
        )
        return result

    async def _write(
        self, dump: DiagnosticDump, path: pathlib.Path, text: str
    ) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, path.write_text, text)
        dump.written_files.append(path)

    async def _write_jsonl(
        self,
        dump: DiagnosticDump,
        path: pathlib.Path,
        records: Iterable[dict[str, Any]],
    ) -> None:
        try:
            lines = [
                json.dumps(record, default=_json_safe) + "\n"
                for record in records
            ]
        except Exception as serialize_error:
            lines = [
                json.dumps(
                    {
                        "snapshot_error": f"{type(serialize_error).__name__}: {serialize_error}",
                    }
                )
                + "\n"
            ]
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, path.write_text, "".join(lines))
        dump.written_files.append(path)

    def _topology_records(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for handle in self.harness.all_handles():
            records.append(
                {
                    "node_id": handle.node_id,
                    "kind": handle.kind.value,
                    "dc_id": handle.dc_id,
                    "tcp_port": handle.tcp_port,
                    "udp_port": handle.udp_port,
                    "started": handle.started,
                }
            )
        return records

    def _node_state_records(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for handle in self.harness.all_handles():
            try:
                records.append(self._snapshot_node(handle))
            except Exception as snapshot_error:
                records.append(
                    {
                        "node_id": handle.node_id,
                        "snapshot_error": f"{type(snapshot_error).__name__}: {snapshot_error}",
                    }
                )
        return records

    def _snapshot_node(self, handle: ServerHandle) -> dict[str, Any]:
        if handle.kind is ServerKind.MANAGER:
            return self._snapshot_manager(handle)
        if handle.kind is ServerKind.WORKER:
            return self._snapshot_worker(handle)
        if handle.kind is ServerKind.GATE:
            return self._snapshot_gate(handle)
        return {"node_id": handle.node_id, "kind": str(handle.kind)}

    def _snapshot_manager(self, handle: ServerHandle) -> dict[str, Any]:
        instance = handle.instance
        state = getattr(instance, "_manager_state", None)
        local_health = getattr(instance, "_local_health", None)
        worker_count = state.get_worker_count() if state else None
        swim_confirmed_worker_count = None
        worker_ids: list[str] | None = None
        if state is not None:
            worker_ids = [worker_id for worker_id, _worker in state.iter_workers()]
            tracker = getattr(instance, "_incarnation_tracker", None)
            if tracker is not None:
                swim_confirmed_worker_count = 0
                for _worker_id, registration in state.iter_workers():
                    worker_udp_addr = (
                        registration.node.host,
                        registration.node.udp_port,
                    )
                    if tracker.is_node_confirmed(worker_udp_addr):
                        swim_confirmed_worker_count += 1

        return {
            "node_id": handle.node_id,
            "kind": "manager",
            "active_peer_count": (
                len(state.get_active_manager_peer_ids()) if state else None
            ),
            "known_peer_count": (
                state.get_known_manager_peer_count() if state else None
            ),
            "worker_count": worker_count,
            "worker_ids": worker_ids,
            "swim_confirmed_worker_count": swim_confirmed_worker_count,
            "lhm_score": local_health.score if local_health else None,
            "is_leader": getattr(instance, "is_leader", lambda: None)(),
        }

    def _snapshot_worker(self, handle: ServerHandle) -> dict[str, Any]:
        instance = handle.instance
        registry = getattr(instance, "_registry", None)
        primary = getattr(registry, "_primary_manager_id", None) if registry else None
        local_health = getattr(instance, "_local_health", None)
        return {
            "node_id": handle.node_id,
            "kind": "worker",
            "primary_manager_id": primary,
            "known_manager_count": (
                len(getattr(registry, "_known_managers", {})) if registry else None
            ),
            "healthy_manager_count": (
                len(getattr(registry, "_healthy_manager_ids", set()))
                if registry
                else None
            ),
            "lhm_score": local_health.score if local_health else None,
            "tracked_subprocess_count": len(
                self.harness.supervisor.tracked_pids(handle.node_id)
            ),
        }

    def _snapshot_gate(self, handle: ServerHandle) -> dict[str, Any]:
        instance = handle.instance
        state = getattr(instance, "_modular_state", None)
        return {
            "node_id": handle.node_id,
            "kind": "gate",
            "active_peer_count": state.get_active_peer_count() if state else None,
            "known_gate_count": state.get_known_gate_count() if state else None,
        }

    def _process_tree_records(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        try:
            self_proc = psutil.Process()
        except psutil.Error as walk_error:
            return [
                {
                    "snapshot_error": f"{type(walk_error).__name__}: {walk_error}",
                }
            ]
        for proc in self_proc.children(recursive=True):
            try:
                with proc.oneshot():
                    records.append(
                        {
                            "pid": proc.pid,
                            "ppid": proc.ppid(),
                            "status": proc.status(),
                            "cmdline": proc.cmdline()[:6],
                            "create_age_seconds": _safe_create_age(proc),
                            "rss_bytes": proc.memory_info().rss,
                        }
                    )
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return records

    def _pid_attribution_records(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for handle in self.harness.all_handles():
            if handle.kind is not ServerKind.WORKER:
                continue
            tracked = self.harness.supervisor.tracked_pids(handle.node_id)
            for pid in sorted(tracked):
                records.append(
                    {
                        "node_id": handle.node_id,
                        "pid": pid,
                        "alive": psutil.pid_exists(pid),
                    }
                )
        return records

    def _asyncio_task_records(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        try:
            tasks = asyncio.all_tasks()
        except RuntimeError:
            return [{"snapshot_error": "no running loop"}]
        for task in tasks:
            records.append(
                {
                    "name": task.get_name(),
                    "done": task.done(),
                    "cancelled": task.cancelled() if task.done() else False,
                }
            )
        return records

    def _supervisor_error_records(self) -> list[dict[str, Any]]:
        return [
            {"index": index, "error": error}
            for index, error in enumerate(self.harness.supervisor.cleanup_errors)
        ]


def _safe_create_age(proc: psutil.Process) -> float | None:
    try:
        import time as _time

        return _time.time() - proc.create_time()
    except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
        return None


def _json_safe(value: Any) -> Any:
    """Coerce non-JSON values into something serializable."""
    if isinstance(value, set):
        return sorted(value)
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    return repr(value)
