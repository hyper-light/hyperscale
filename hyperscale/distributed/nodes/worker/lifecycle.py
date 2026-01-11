"""
Worker lifecycle management.

Handles startup, shutdown, and abort operations for WorkerServer.
Extracted from worker_impl.py for modularity (AD-33 compliance).
"""

import asyncio
from multiprocessing import active_children
from typing import TYPE_CHECKING

from hyperscale.core.jobs.graphs.remote_graph_manager import RemoteGraphManager
from hyperscale.core.jobs.runner.local_server_pool import LocalServerPool
from hyperscale.core.monitoring import CPUMonitor, MemoryMonitor
from hyperscale.core.engines.client.time_parser import TimeParser
from hyperscale.core.jobs.models import Env as LocalEnv
from hyperscale.distributed.protocol.version import NodeCapabilities
from hyperscale.logging.config.logging_config import LoggingConfig
from hyperscale.logging.hyperscale_logging_models import ServerError, ServerInfo

if TYPE_CHECKING:
    from hyperscale.distributed.env import Env
    from hyperscale.logging import Logger


class WorkerLifecycleManager:
    """
    Manages worker server lifecycle operations.

    Handles startup sequence including monitors, pools, registration,
    and background loops. Handles graceful and emergency shutdown.
    """

    def __init__(
        self,
        host: str,
        tcp_port: int,
        udp_port: int,
        total_cores: int,
        env: "Env",
        logger: "Logger | None" = None,
    ) -> None:
        """
        Initialize lifecycle manager.

        Args:
            host: Worker host address
            tcp_port: Worker TCP port
            udp_port: Worker UDP port
            total_cores: Total CPU cores available
            env: Environment configuration
            logger: Logger instance
        """
        self._host = host
        self._tcp_port = tcp_port
        self._udp_port = udp_port
        self._total_cores = total_cores
        self._env = env
        self._logger = logger

        # Compute derived ports
        self._local_udp_port = udp_port + (total_cores ** 2)

        # Initialize monitors
        self._cpu_monitor = CPUMonitor(env)
        self._memory_monitor = MemoryMonitor(env)

        # Initialize server pool and remote manager
        self._server_pool = LocalServerPool(total_cores)
        self._remote_manager: RemoteGraphManager | None = None

        # Logging configuration
        self._logging_config: LoggingConfig | None = None

        # Connection timeout
        self._connect_timeout = TimeParser(env.MERCURY_SYNC_CONNECT_SECONDS).time

        # Local env for worker processes
        self._local_env = LocalEnv(
            MERCURY_SYNC_AUTH_SECRET=env.MERCURY_SYNC_AUTH_SECRET
        )

        # Background task references
        self._background_tasks: list[asyncio.Task] = []

        # State flags
        self._started = False
        self._running = False

    def get_worker_ips(self) -> list[tuple[str, int]]:
        """Get list of worker IP/port tuples for local processes."""
        base_worker_port = self._local_udp_port + (self._total_cores ** 2)
        return [
            (self._host, port)
            for port in range(
                base_worker_port,
                base_worker_port + (self._total_cores ** 2),
                self._total_cores,
            )
        ]

    async def initialize_remote_manager(
        self,
        updates_controller,
        status_update_poll_interval: float,
    ) -> RemoteGraphManager:
        """
        Initialize and return the RemoteGraphManager.

        Args:
            updates_controller: InterfaceUpdatesController instance
            status_update_poll_interval: Poll interval for status updates

        Returns:
            Initialized RemoteGraphManager
        """
        self._remote_manager = RemoteGraphManager(
            updates_controller,
            self._total_cores,
            status_update_poll_interval=status_update_poll_interval,
        )
        return self._remote_manager

    async def start_monitors(
        self,
        datacenter_id: str,
        node_id: str,
    ) -> None:
        """
        Start CPU and memory monitors.

        Args:
            datacenter_id: Datacenter identifier
            node_id: Full node identifier
        """
        await self._cpu_monitor.start_background_monitor(datacenter_id, node_id)
        await self._memory_monitor.start_background_monitor(datacenter_id, node_id)

    async def stop_monitors(
        self,
        datacenter_id: str,
        node_id: str,
    ) -> None:
        """
        Stop CPU and memory monitors.

        Args:
            datacenter_id: Datacenter identifier
            node_id: Full node identifier
        """
        await self._cpu_monitor.stop_background_monitor(datacenter_id, node_id)
        await self._memory_monitor.stop_background_monitor(datacenter_id, node_id)

    async def setup_server_pool(self) -> None:
        """Set up the local server pool."""
        await self._server_pool.setup()

    async def start_remote_manager(self) -> None:
        """Start the remote graph manager."""
        if not self._remote_manager:
            raise RuntimeError("RemoteGraphManager not initialized")

        await self._remote_manager.start(
            self._host,
            self._local_udp_port,
            self._local_env,
        )

    async def run_worker_pool(self) -> None:
        """Run the local worker process pool."""
        if not self._remote_manager:
            raise RuntimeError("RemoteGraphManager not initialized")

        worker_ips = self.get_worker_ips()
        await self._server_pool.run_pool(
            (self._host, self._local_udp_port),
            worker_ips,
            self._local_env,
            enable_server_cleanup=True,
        )

    async def connect_to_workers(
        self,
        timeout: float | None = None,
    ) -> None:
        """
        Connect to local worker processes.

        Args:
            timeout: Connection timeout (uses default if None)

        Raises:
            RuntimeError: If connection times out
        """
        if not self._remote_manager:
            raise RuntimeError("RemoteGraphManager not initialized")

        effective_timeout = timeout or self._connect_timeout
        worker_ips = self.get_worker_ips()

        try:
            await asyncio.wait_for(
                self._remote_manager.connect_to_workers(
                    worker_ips,
                    timeout=effective_timeout,
                ),
                timeout=effective_timeout + 10.0,
            )
        except asyncio.TimeoutError:
            raise RuntimeError(
                f"Worker process pool failed to start within {effective_timeout + 10.0}s. "
                "Check logs for process spawn errors."
            )

    def set_on_cores_available(self, callback: callable) -> None:
        """
        Register callback for core availability notifications.

        Args:
            callback: Function to call when cores become available
        """
        if self._remote_manager:
            self._remote_manager.set_on_cores_available(callback)

    def setup_logging_config(self) -> None:
        """Set up logging configuration from environment."""
        if self._logging_config is None:
            self._logging_config = LoggingConfig()
            self._logging_config.update(
                log_directory=self._env.MERCURY_SYNC_LOGS_DIRECTORY,
                log_level=self._env.MERCURY_SYNC_LOG_LEVEL,
            )

    def get_node_capabilities(self, node_version: str) -> NodeCapabilities:
        """
        Get node capabilities for protocol negotiation.

        Args:
            node_version: Version string for this node

        Returns:
            NodeCapabilities instance
        """
        return NodeCapabilities.current(node_version=node_version)

    def add_background_task(self, task: asyncio.Task) -> None:
        """
        Track a background task for cleanup during shutdown.

        Args:
            task: Background task to track
        """
        self._background_tasks.append(task)

    async def cancel_background_tasks(self) -> None:
        """Cancel all tracked background tasks."""
        for task in self._background_tasks:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self._background_tasks.clear()

    def cancel_background_tasks_sync(self) -> None:
        """Cancel all tracked background tasks synchronously (for abort)."""
        for task in self._background_tasks:
            if task and not task.done():
                task.cancel()

        self._background_tasks.clear()

    async def shutdown_remote_manager(self) -> None:
        """Shut down the remote graph manager and workers."""
        if self._remote_manager:
            await self._remote_manager.shutdown_workers()
            await self._remote_manager.close()

    async def shutdown_server_pool(self) -> None:
        """Shut down the local server pool."""
        await self._server_pool.shutdown()

    async def kill_child_processes(self) -> None:
        """Kill any remaining child processes."""
        try:
            loop = asyncio.get_running_loop()
            children = await loop.run_in_executor(None, active_children)
            if children:
                await asyncio.gather(
                    *[loop.run_in_executor(None, child.kill) for child in children]
                )
        except RuntimeError:
            for child in active_children():
                try:
                    child.kill()
                except Exception:
                    pass

    def abort_monitors(self) -> None:
        """Abort all monitors (emergency shutdown)."""
        try:
            self._cpu_monitor.abort_all_background_monitors()
        except Exception:
            pass

        try:
            self._memory_monitor.abort_all_background_monitors()
        except Exception:
            pass

    def abort_remote_manager(self) -> None:
        """Abort remote manager (emergency shutdown)."""
        if self._remote_manager:
            try:
                self._remote_manager.abort()
            except Exception:
                pass

    def abort_server_pool(self) -> None:
        """Abort server pool (emergency shutdown)."""
        try:
            self._server_pool.abort()
        except Exception:
            pass

    def get_monitor_averages(
        self,
        run_id: int,
        workflow_name: str,
    ) -> tuple[float, float]:
        """
        Get CPU and memory moving averages for a workflow.

        Args:
            run_id: Workflow run identifier
            workflow_name: Workflow name

        Returns:
            Tuple of (cpu_avg, memory_avg)
        """
        cpu_avg = self._cpu_monitor.get_moving_avg(run_id, workflow_name)
        memory_avg = self._memory_monitor.get_moving_avg(run_id, workflow_name)
        return (cpu_avg, memory_avg)

    def get_availability(self) -> tuple[int, int, int]:
        """
        Get workflow core availability from remote manager.

        Returns:
            Tuple of (assigned_cores, completed_cores, available_cores)
        """
        if not self._remote_manager:
            return (0, 0, 0)
        return self._remote_manager.get_availability()

    def start_server_cleanup(self) -> None:
        """Trigger server cleanup in remote manager."""
        if self._remote_manager:
            self._remote_manager.start_server_cleanup()

    @property
    def remote_manager(self) -> RemoteGraphManager | None:
        """Get remote graph manager instance."""
        return self._remote_manager

    @property
    def cpu_monitor(self) -> CPUMonitor:
        """Get CPU monitor instance."""
        return self._cpu_monitor

    @property
    def memory_monitor(self) -> MemoryMonitor:
        """Get memory monitor instance."""
        return self._memory_monitor