"""
Integration tests for WorkerLifecycleManager (Section 15.2.7).

Tests WorkerLifecycleManager for startup, shutdown, and resource management.

Covers:
- Happy path: Normal startup and shutdown sequences
- Negative path: Invalid configurations
- Failure mode: Component failures during startup/shutdown
- Concurrency: Thread-safe task management
- Edge cases: Zero cores, timeout handling
"""

import asyncio
from unittest.mock import MagicMock, AsyncMock, patch

import pytest

from hyperscale.distributed.nodes.worker.lifecycle import WorkerLifecycleManager


class MockEnv:
    """Mock Env for lifecycle manager testing."""

    def __init__(self):
        self.MERCURY_SYNC_AUTH_SECRET = "test-secret"
        self.MERCURY_SYNC_LOGS_DIRECTORY = "/tmp/logs"
        self.MERCURY_SYNC_LOG_LEVEL = "INFO"
        self.MERCURY_SYNC_CONNECT_SECONDS = "30s"
        self.MERCURY_SYNC_MONITOR_SAMPLE_WINDOW = "5s"
        self.MERCURY_SYNC_MONITOR_SAMPLE_INTERVAL = 0.1
        self.MERCURY_SYNC_PROCESS_JOB_CPU_LIMIT = 85
        self.MERCURY_SYNC_PROCESS_JOB_MEMORY_LIMIT = 2048


class TestWorkerLifecycleManagerInitialization:
    """Test WorkerLifecycleManager initialization."""

    def test_happy_path_instantiation(self) -> None:
        """Test normal instantiation."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        assert manager._host == "localhost"
        assert manager._tcp_port == 8000
        assert manager._udp_port == 8001
        assert manager._total_cores == 4
        assert manager._env == env
        assert manager._started is False
        assert manager._running is False

    def test_with_logger(self) -> None:
        """Test with logger provided."""
        env = MockEnv()
        logger = MagicMock()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
            logger=logger,
        )

        assert manager._logger == logger

    def test_local_udp_port_calculation(self) -> None:
        """Test local UDP port is calculated from udp_port and total_cores."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        # local_udp_port = udp_port + (total_cores ** 2)
        expected_local_udp_port = 8001 + (4 ** 2)
        assert manager._local_udp_port == expected_local_udp_port


class TestWorkerLifecycleManagerWorkerIPs:
    """Test worker IP generation."""

    def test_get_worker_ips(self) -> None:
        """Test generating worker IP tuples."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="192.168.1.1",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        worker_ips = manager.get_worker_ips()

        # Should have multiple worker IPs based on total_cores
        assert len(worker_ips) > 0
        assert all(ip[0] == "192.168.1.1" for ip in worker_ips)
        assert all(isinstance(ip[1], int) for ip in worker_ips)

    def test_get_worker_ips_single_core(self) -> None:
        """Test worker IPs with single core."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=1,
            env=env,
        )

        worker_ips = manager.get_worker_ips()
        assert len(worker_ips) == 1


class TestWorkerLifecycleManagerMonitors:
    """Test monitor management."""

    @pytest.mark.asyncio
    async def test_start_monitors(self) -> None:
        """Test starting CPU and memory monitors."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        # Mock the monitors
        manager._cpu_monitor = MagicMock()
        manager._cpu_monitor.start_background_monitor = AsyncMock()
        manager._memory_monitor = MagicMock()
        manager._memory_monitor.start_background_monitor = AsyncMock()

        await manager.start_monitors("dc-1", "node-123")

        manager._cpu_monitor.start_background_monitor.assert_awaited_once_with("dc-1", "node-123")
        manager._memory_monitor.start_background_monitor.assert_awaited_once_with("dc-1", "node-123")

    @pytest.mark.asyncio
    async def test_stop_monitors(self) -> None:
        """Test stopping CPU and memory monitors."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        # Mock the monitors
        manager._cpu_monitor = MagicMock()
        manager._cpu_monitor.stop_background_monitor = AsyncMock()
        manager._memory_monitor = MagicMock()
        manager._memory_monitor.stop_background_monitor = AsyncMock()

        await manager.stop_monitors("dc-1", "node-123")

        manager._cpu_monitor.stop_background_monitor.assert_awaited_once_with("dc-1", "node-123")
        manager._memory_monitor.stop_background_monitor.assert_awaited_once_with("dc-1", "node-123")

    def test_abort_monitors(self) -> None:
        """Test aborting monitors (emergency shutdown)."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        # Mock the monitors
        manager._cpu_monitor = MagicMock()
        manager._memory_monitor = MagicMock()

        # Should not raise even if monitors fail
        manager.abort_monitors()

        manager._cpu_monitor.abort_all_background_monitors.assert_called_once()
        manager._memory_monitor.abort_all_background_monitors.assert_called_once()


class TestWorkerLifecycleManagerBackgroundTasks:
    """Test background task management."""

    def test_add_background_task(self) -> None:
        """Test adding background task for tracking."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        task = MagicMock()
        manager.add_background_task(task)

        assert len(manager._background_tasks) == 1
        assert manager._background_tasks[0] is task

    @pytest.mark.asyncio
    async def test_cancel_background_tasks(self) -> None:
        """Test cancelling all background tasks."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        # Create mock tasks
        task1 = MagicMock()
        task1.done.return_value = False
        task1.cancel = MagicMock()

        task2 = MagicMock()
        task2.done.return_value = True  # Already done
        task2.cancel = MagicMock()

        # Use real async function for awaiting cancelled task
        async def cancelled_coro():
            raise asyncio.CancelledError()

        task1.__await__ = cancelled_coro().__await__

        manager.add_background_task(task1)
        manager.add_background_task(task2)

        await manager.cancel_background_tasks()

        task1.cancel.assert_called_once()
        task2.cancel.assert_not_called()  # Already done, shouldn't cancel
        assert len(manager._background_tasks) == 0

    def test_cancel_background_tasks_sync(self) -> None:
        """Test synchronous background task cancellation (for abort)."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        task1 = MagicMock()
        task1.done.return_value = False
        task2 = MagicMock()
        task2.done.return_value = True

        manager.add_background_task(task1)
        manager.add_background_task(task2)

        manager.cancel_background_tasks_sync()

        task1.cancel.assert_called_once()
        task2.cancel.assert_not_called()
        assert len(manager._background_tasks) == 0


class TestWorkerLifecycleManagerRemoteManager:
    """Test RemoteGraphManager integration."""

    @pytest.mark.asyncio
    async def test_initialize_remote_manager(self) -> None:
        """Test initializing RemoteGraphManager."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        updates_controller = MagicMock()

        with patch("hyperscale.distributed.nodes.worker.lifecycle.RemoteGraphManager") as mock_rgm:
            mock_instance = MagicMock()
            mock_rgm.return_value = mock_instance

            result = await manager.initialize_remote_manager(updates_controller, 1.0)

            mock_rgm.assert_called_once_with(updates_controller, 4, status_update_poll_interval=1.0)
            assert result is mock_instance
            assert manager._remote_manager is mock_instance

    @pytest.mark.asyncio
    async def test_start_remote_manager(self) -> None:
        """Test starting RemoteGraphManager."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        # Set up remote manager
        manager._remote_manager = MagicMock()
        manager._remote_manager.start = AsyncMock()

        await manager.start_remote_manager()

        manager._remote_manager.start.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_start_remote_manager_not_initialized(self) -> None:
        """Test starting RemoteGraphManager when not initialized."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        with pytest.raises(RuntimeError, match="not initialized"):
            await manager.start_remote_manager()

    @pytest.mark.asyncio
    async def test_shutdown_remote_manager(self) -> None:
        """Test shutting down RemoteGraphManager."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        manager._remote_manager = MagicMock()
        manager._remote_manager.shutdown_workers = AsyncMock()
        manager._remote_manager.close = AsyncMock()

        await manager.shutdown_remote_manager()

        manager._remote_manager.shutdown_workers.assert_awaited_once()
        manager._remote_manager.close.assert_awaited_once()

    def test_abort_remote_manager(self) -> None:
        """Test aborting RemoteGraphManager."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        manager._remote_manager = MagicMock()

        manager.abort_remote_manager()

        manager._remote_manager.abort.assert_called_once()

    def test_abort_remote_manager_not_initialized(self) -> None:
        """Test aborting when not initialized (should not raise)."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        # Should not raise
        manager.abort_remote_manager()


class TestWorkerLifecycleManagerServerPool:
    """Test server pool management."""

    @pytest.mark.asyncio
    async def test_setup_server_pool(self) -> None:
        """Test setting up server pool."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        manager._server_pool = MagicMock()
        manager._server_pool.setup = AsyncMock()

        await manager.setup_server_pool()

        manager._server_pool.setup.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_shutdown_server_pool(self) -> None:
        """Test shutting down server pool."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        manager._server_pool = MagicMock()
        manager._server_pool.shutdown = AsyncMock()

        await manager.shutdown_server_pool()

        manager._server_pool.shutdown.assert_awaited_once()

    def test_abort_server_pool(self) -> None:
        """Test aborting server pool."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        manager._server_pool = MagicMock()

        manager.abort_server_pool()

        manager._server_pool.abort.assert_called_once()


class TestWorkerLifecycleManagerCapabilities:
    """Test node capabilities."""

    def test_get_node_capabilities(self) -> None:
        """Test getting node capabilities."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        capabilities = manager.get_node_capabilities("1.0.0")

        assert capabilities is not None
        assert capabilities.protocol_version is not None

    def test_setup_logging_config(self) -> None:
        """Test setting up logging configuration."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        manager.setup_logging_config()

        assert manager._logging_config is not None


class TestWorkerLifecycleManagerMetrics:
    """Test metrics collection."""

    def test_get_monitor_averages(self) -> None:
        """Test getting CPU and memory averages."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        manager._cpu_monitor = MagicMock()
        manager._cpu_monitor.get_moving_avg.return_value = 50.0
        manager._memory_monitor = MagicMock()
        manager._memory_monitor.get_moving_avg.return_value = 60.0

        cpu_avg, memory_avg = manager.get_monitor_averages(1, "test-workflow")

        assert cpu_avg == 50.0
        assert memory_avg == 60.0
        manager._cpu_monitor.get_moving_avg.assert_called_once_with(1, "test-workflow")
        manager._memory_monitor.get_moving_avg.assert_called_once_with(1, "test-workflow")

    def test_get_availability(self) -> None:
        """Test getting core availability."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        manager._remote_manager = MagicMock()
        manager._remote_manager.get_availability.return_value = (2, 1, 1)

        result = manager.get_availability()

        assert result == (2, 1, 1)

    def test_get_availability_no_remote_manager(self) -> None:
        """Test getting availability when remote manager not initialized."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        result = manager.get_availability()

        assert result == (0, 0, 0)


class TestWorkerLifecycleManagerCallbacks:
    """Test callback registration."""

    def test_set_on_cores_available(self) -> None:
        """Test setting core availability callback."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        manager._remote_manager = MagicMock()
        callback = MagicMock()

        manager.set_on_cores_available(callback)

        manager._remote_manager.set_on_cores_available.assert_called_once_with(callback)


class TestWorkerLifecycleManagerProperties:
    """Test property access."""

    def test_remote_manager_property(self) -> None:
        """Test remote_manager property."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        assert manager.remote_manager is None

        mock_rm = MagicMock()
        manager._remote_manager = mock_rm
        assert manager.remote_manager is mock_rm

    def test_cpu_monitor_property(self) -> None:
        """Test cpu_monitor property."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        assert manager.cpu_monitor is not None

    def test_memory_monitor_property(self) -> None:
        """Test memory_monitor property."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        assert manager.memory_monitor is not None


class TestWorkerLifecycleManagerEdgeCases:
    """Test edge cases."""

    def test_zero_cores(self) -> None:
        """Test with zero cores (invalid but should not crash)."""
        env = MockEnv()
        # This might be an invalid state, but should handle gracefully
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=0,
            env=env,
        )

        assert manager._total_cores == 0
        # Worker IPs should be empty or handle zero cores
        worker_ips = manager.get_worker_ips()
        assert isinstance(worker_ips, list)

    def test_many_cores(self) -> None:
        """Test with many cores."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=128,
            env=env,
        )

        worker_ips = manager.get_worker_ips()
        assert len(worker_ips) > 0

    @pytest.mark.asyncio
    async def test_kill_child_processes(self) -> None:
        """Test killing child processes."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        # Should not raise even if no children
        await manager.kill_child_processes()

    def test_start_server_cleanup(self) -> None:
        """Test triggering server cleanup."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        manager._remote_manager = MagicMock()

        manager.start_server_cleanup()

        manager._remote_manager.start_server_cleanup.assert_called_once()


class TestWorkerLifecycleManagerFailureModes:
    """Test failure modes."""

    def test_abort_monitors_with_exception(self) -> None:
        """Test abort_monitors handles exceptions gracefully."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        manager._cpu_monitor = MagicMock()
        manager._cpu_monitor.abort_all_background_monitors.side_effect = RuntimeError("Abort failed")
        manager._memory_monitor = MagicMock()

        # Should not raise
        manager.abort_monitors()

        # Memory monitor should still be called
        manager._memory_monitor.abort_all_background_monitors.assert_called_once()

    def test_abort_remote_manager_with_exception(self) -> None:
        """Test abort_remote_manager handles exceptions gracefully."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        manager._remote_manager = MagicMock()
        manager._remote_manager.abort.side_effect = RuntimeError("Abort failed")

        # Should not raise
        manager.abort_remote_manager()

    def test_abort_server_pool_with_exception(self) -> None:
        """Test abort_server_pool handles exceptions gracefully."""
        env = MockEnv()
        manager = WorkerLifecycleManager(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=4,
            env=env,
        )

        manager._server_pool = MagicMock()
        manager._server_pool.abort.side_effect = RuntimeError("Abort failed")

        # Should not raise
        manager.abort_server_pool()
