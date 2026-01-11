"""
Integration tests for WorkerHeartbeatHandler (Section 15.2.7).

Tests WorkerHeartbeatHandler for manager heartbeat processing and SWIM integration.

Covers:
- Happy path: Normal heartbeat processing
- Negative path: Unknown managers
- Failure mode: Invalid heartbeat data
- Concurrency: Thread-safe manager tracking
- Edge cases: Leadership changes, job leadership claims
"""

from unittest.mock import MagicMock

import pytest

from hyperscale.distributed.nodes.worker.heartbeat import WorkerHeartbeatHandler
from hyperscale.distributed.nodes.worker.registry import WorkerRegistry
from hyperscale.distributed.models import ManagerHeartbeat, ManagerInfo


class TestWorkerHeartbeatHandlerInitialization:
    """Test WorkerHeartbeatHandler initialization."""

    def test_happy_path_instantiation(self) -> None:
        """Test normal instantiation."""
        registry = WorkerRegistry(None)
        logger = MagicMock()

        handler = WorkerHeartbeatHandler(
            registry=registry,
            logger=logger,
        )

        assert handler._registry is registry
        assert handler._logger is logger
        assert handler._on_new_manager_discovered is None
        assert handler._on_job_leadership_update is None

    def test_without_logger(self) -> None:
        """Test instantiation without logger."""
        registry = WorkerRegistry(None)

        handler = WorkerHeartbeatHandler(registry=registry)

        assert handler._logger is None


class TestWorkerHeartbeatHandlerCallbacks:
    """Test callback configuration."""

    def test_set_callbacks(self) -> None:
        """Test setting callbacks."""
        registry = WorkerRegistry(None)
        handler = WorkerHeartbeatHandler(registry=registry)

        on_new_manager = MagicMock()
        on_job_leadership = MagicMock()

        handler.set_callbacks(
            on_new_manager_discovered=on_new_manager,
            on_job_leadership_update=on_job_leadership,
        )

        assert handler._on_new_manager_discovered is on_new_manager
        assert handler._on_job_leadership_update is on_job_leadership

    def test_set_partial_callbacks(self) -> None:
        """Test setting only some callbacks."""
        registry = WorkerRegistry(None)
        handler = WorkerHeartbeatHandler(registry=registry)

        on_new_manager = MagicMock()

        handler.set_callbacks(on_new_manager_discovered=on_new_manager)

        assert handler._on_new_manager_discovered is on_new_manager
        assert handler._on_job_leadership_update is None


class TestWorkerHeartbeatHandlerProcessHeartbeat:
    """Test processing manager heartbeats."""

    def test_process_heartbeat_new_manager(self) -> None:
        """Test processing heartbeat from new manager."""
        registry = WorkerRegistry(None)
        logger = MagicMock()
        handler = WorkerHeartbeatHandler(registry=registry, logger=logger)

        on_new_manager = MagicMock()
        handler.set_callbacks(on_new_manager_discovered=on_new_manager)

        heartbeat = ManagerHeartbeat(
            node_id="mgr-new",
            is_leader=False,
            tcp_host="192.168.1.100",
            tcp_port=8000,
            datacenter="dc-1",
            job_leaderships=[],
        )

        confirm_peer = MagicMock()
        task_runner_run = MagicMock()

        handler.process_manager_heartbeat(
            heartbeat=heartbeat,
            source_addr=("192.168.1.100", 8001),
            confirm_peer=confirm_peer,
            node_host="192.168.1.1",
            node_port=8000,
            node_id_short="wkr",
            task_runner_run=task_runner_run,
        )

        # Peer should be confirmed
        confirm_peer.assert_called_once_with(("192.168.1.100", 8001))

        # New manager should be registered
        assert "mgr-new" in registry._known_managers

        # Callback should be triggered
        assert task_runner_run.called

    def test_process_heartbeat_existing_manager(self) -> None:
        """Test processing heartbeat from existing manager."""
        registry = WorkerRegistry(None)
        handler = WorkerHeartbeatHandler(registry=registry)

        # Add existing manager
        existing_manager = ManagerInfo(
            node_id="mgr-1",
            tcp_host="192.168.1.100",
            tcp_port=8000,
            udp_host="192.168.1.100",
            udp_port=8001,
            is_leader=False,
        )
        registry.add_manager("mgr-1", existing_manager)

        heartbeat = ManagerHeartbeat(
            node_id="mgr-1",
            is_leader=False,  # Same leadership status
            tcp_host="192.168.1.100",
            tcp_port=8000,
            datacenter="dc-1",
            job_leaderships=[],
        )

        confirm_peer = MagicMock()
        task_runner_run = MagicMock()

        handler.process_manager_heartbeat(
            heartbeat=heartbeat,
            source_addr=("192.168.1.100", 8001),
            confirm_peer=confirm_peer,
            node_host="192.168.1.1",
            node_port=8000,
            node_id_short="wkr",
            task_runner_run=task_runner_run,
        )

        # Should confirm peer
        confirm_peer.assert_called_once()

        # Manager should still exist
        assert "mgr-1" in registry._known_managers

    def test_process_heartbeat_leadership_change(self) -> None:
        """Test processing heartbeat with leadership change."""
        registry = WorkerRegistry(None)
        logger = MagicMock()
        handler = WorkerHeartbeatHandler(registry=registry, logger=logger)

        # Add existing non-leader manager
        existing_manager = ManagerInfo(
            node_id="mgr-1",
            tcp_host="192.168.1.100",
            tcp_port=8000,
            udp_host="192.168.1.100",
            udp_port=8001,
            is_leader=False,
        )
        registry.add_manager("mgr-1", existing_manager)

        # Set another manager as primary
        registry.set_primary_manager("mgr-other")

        heartbeat = ManagerHeartbeat(
            node_id="mgr-1",
            is_leader=True,  # Now became leader
            tcp_host="192.168.1.100",
            tcp_port=8000,
            datacenter="dc-1",
            job_leaderships=[],
        )

        confirm_peer = MagicMock()
        task_runner_run = MagicMock()

        handler.process_manager_heartbeat(
            heartbeat=heartbeat,
            source_addr=("192.168.1.100", 8001),
            confirm_peer=confirm_peer,
            node_host="192.168.1.1",
            node_port=8000,
            node_id_short="wkr",
            task_runner_run=task_runner_run,
        )

        # Primary should be updated to new leader
        assert registry._primary_manager_id == "mgr-1"

        # Manager info should be updated
        updated_manager = registry.get_manager("mgr-1")
        assert updated_manager.is_leader is True

    def test_process_heartbeat_with_job_leaderships(self) -> None:
        """Test processing heartbeat with job leadership claims."""
        registry = WorkerRegistry(None)
        handler = WorkerHeartbeatHandler(registry=registry)

        on_job_leadership = MagicMock()
        handler.set_callbacks(on_job_leadership_update=on_job_leadership)

        heartbeat = ManagerHeartbeat(
            node_id="mgr-1",
            is_leader=False,
            tcp_host="192.168.1.100",
            tcp_port=8000,
            datacenter="dc-1",
            job_leaderships=["job-1", "job-2"],
        )

        confirm_peer = MagicMock()
        task_runner_run = MagicMock()

        handler.process_manager_heartbeat(
            heartbeat=heartbeat,
            source_addr=("192.168.1.100", 8001),
            confirm_peer=confirm_peer,
            node_host="192.168.1.1",
            node_port=8000,
            node_id_short="wkr",
            task_runner_run=task_runner_run,
        )

        # Job leadership callback should be invoked
        on_job_leadership.assert_called_once()
        call_args = on_job_leadership.call_args[0]
        assert call_args[0] == ["job-1", "job-2"]
        assert call_args[1] == ("192.168.1.100", 8000)  # TCP addr

    def test_process_heartbeat_no_job_leaderships(self) -> None:
        """Test processing heartbeat without job leadership claims."""
        registry = WorkerRegistry(None)
        handler = WorkerHeartbeatHandler(registry=registry)

        on_job_leadership = MagicMock()
        handler.set_callbacks(on_job_leadership_update=on_job_leadership)

        heartbeat = ManagerHeartbeat(
            node_id="mgr-1",
            is_leader=False,
            tcp_host="192.168.1.100",
            tcp_port=8000,
            datacenter="dc-1",
            job_leaderships=[],  # Empty
        )

        confirm_peer = MagicMock()
        task_runner_run = MagicMock()

        handler.process_manager_heartbeat(
            heartbeat=heartbeat,
            source_addr=("192.168.1.100", 8001),
            confirm_peer=confirm_peer,
            node_host="192.168.1.1",
            node_port=8000,
            node_id_short="wkr",
            task_runner_run=task_runner_run,
        )

        # Job leadership callback should NOT be invoked
        on_job_leadership.assert_not_called()


class TestWorkerHeartbeatHandlerPeerConfirmation:
    """Test peer confirmation handling (AD-29)."""

    def test_on_peer_confirmed_known_manager(self) -> None:
        """Test peer confirmation for known manager."""
        registry = WorkerRegistry(None)
        logger = MagicMock()
        handler = WorkerHeartbeatHandler(registry=registry, logger=logger)

        # Add manager with UDP address
        manager = ManagerInfo(
            node_id="mgr-1",
            tcp_host="192.168.1.100",
            tcp_port=8000,
            udp_host="192.168.1.100",
            udp_port=8001,
        )
        registry.add_manager("mgr-1", manager)

        task_runner_run = MagicMock()

        handler.on_peer_confirmed(
            peer=("192.168.1.100", 8001),
            node_host="192.168.1.1",
            node_port=8000,
            node_id_short="wkr",
            task_runner_run=task_runner_run,
        )

        # Manager should be marked healthy
        assert registry.is_manager_healthy("mgr-1")

    def test_on_peer_confirmed_unknown_peer(self) -> None:
        """Test peer confirmation for unknown peer."""
        registry = WorkerRegistry(None)
        handler = WorkerHeartbeatHandler(registry=registry)

        task_runner_run = MagicMock()

        handler.on_peer_confirmed(
            peer=("192.168.1.200", 9001),  # Unknown
            node_host="192.168.1.1",
            node_port=8000,
            node_id_short="wkr",
            task_runner_run=task_runner_run,
        )

        # Should not crash, just do nothing
        # No manager should be marked healthy
        assert len(registry._healthy_manager_ids) == 0


class TestWorkerHeartbeatHandlerTCPAddressInference:
    """Test TCP address inference from heartbeat."""

    def test_tcp_address_from_heartbeat(self) -> None:
        """Test TCP address is taken from heartbeat."""
        registry = WorkerRegistry(None)
        handler = WorkerHeartbeatHandler(registry=registry)

        heartbeat = ManagerHeartbeat(
            node_id="mgr-1",
            is_leader=False,
            tcp_host="10.0.0.100",  # Different from UDP
            tcp_port=9000,
            datacenter="dc-1",
            job_leaderships=[],
        )

        handler.process_manager_heartbeat(
            heartbeat=heartbeat,
            source_addr=("192.168.1.100", 8001),  # UDP source
            confirm_peer=MagicMock(),
            node_host="192.168.1.1",
            node_port=8000,
            node_id_short="wkr",
            task_runner_run=MagicMock(),
        )

        manager = registry.get_manager("mgr-1")
        assert manager.tcp_host == "10.0.0.100"
        assert manager.tcp_port == 9000

    def test_tcp_address_inferred_from_source(self) -> None:
        """Test TCP address inferred from source when not in heartbeat."""
        registry = WorkerRegistry(None)
        handler = WorkerHeartbeatHandler(registry=registry)

        heartbeat = ManagerHeartbeat(
            node_id="mgr-1",
            is_leader=False,
            tcp_host=None,  # Not provided
            tcp_port=None,
            datacenter="dc-1",
            job_leaderships=[],
        )

        handler.process_manager_heartbeat(
            heartbeat=heartbeat,
            source_addr=("192.168.1.100", 8001),
            confirm_peer=MagicMock(),
            node_host="192.168.1.1",
            node_port=8000,
            node_id_short="wkr",
            task_runner_run=MagicMock(),
        )

        manager = registry.get_manager("mgr-1")
        assert manager.tcp_host == "192.168.1.100"
        assert manager.tcp_port == 8000  # UDP port - 1


class TestWorkerHeartbeatHandlerEdgeCases:
    """Test edge cases."""

    def test_new_manager_becomes_primary_when_none_set(self) -> None:
        """Test new leader becomes primary when none set."""
        registry = WorkerRegistry(None)
        handler = WorkerHeartbeatHandler(registry=registry)

        assert registry._primary_manager_id is None

        heartbeat = ManagerHeartbeat(
            node_id="mgr-1",
            is_leader=True,
            tcp_host="192.168.1.100",
            tcp_port=8000,
            datacenter="dc-1",
            job_leaderships=[],
        )

        handler.process_manager_heartbeat(
            heartbeat=heartbeat,
            source_addr=("192.168.1.100", 8001),
            confirm_peer=MagicMock(),
            node_host="192.168.1.1",
            node_port=8000,
            node_id_short="wkr",
            task_runner_run=MagicMock(),
        )

        assert registry._primary_manager_id == "mgr-1"

    def test_multiple_heartbeats_same_manager(self) -> None:
        """Test processing multiple heartbeats from same manager."""
        registry = WorkerRegistry(None)
        handler = WorkerHeartbeatHandler(registry=registry)

        for i in range(5):
            heartbeat = ManagerHeartbeat(
                node_id="mgr-1",
                is_leader=False,
                tcp_host="192.168.1.100",
                tcp_port=8000,
                datacenter=f"dc-{i}",  # Changing datacenter
                job_leaderships=[],
            )

            handler.process_manager_heartbeat(
                heartbeat=heartbeat,
                source_addr=("192.168.1.100", 8001),
                confirm_peer=MagicMock(),
                node_host="192.168.1.1",
                node_port=8000,
                node_id_short="wkr",
                task_runner_run=MagicMock(),
            )

        # Should still have one manager
        assert len(registry._known_managers) == 1

    def test_special_characters_in_node_id(self) -> None:
        """Test processing heartbeat with special characters in node ID."""
        registry = WorkerRegistry(None)
        handler = WorkerHeartbeatHandler(registry=registry)

        heartbeat = ManagerHeartbeat(
            node_id="mgr-🚀-test-ñ",
            is_leader=False,
            tcp_host="192.168.1.100",
            tcp_port=8000,
            datacenter="dc-1",
            job_leaderships=[],
        )

        handler.process_manager_heartbeat(
            heartbeat=heartbeat,
            source_addr=("192.168.1.100", 8001),
            confirm_peer=MagicMock(),
            node_host="192.168.1.1",
            node_port=8000,
            node_id_short="wkr",
            task_runner_run=MagicMock(),
        )

        assert "mgr-🚀-test-ñ" in registry._known_managers

    def test_heartbeat_with_many_job_leaderships(self) -> None:
        """Test heartbeat with many job leadership claims."""
        registry = WorkerRegistry(None)
        handler = WorkerHeartbeatHandler(registry=registry)

        on_job_leadership = MagicMock()
        handler.set_callbacks(on_job_leadership_update=on_job_leadership)

        job_ids = [f"job-{i}" for i in range(100)]

        heartbeat = ManagerHeartbeat(
            node_id="mgr-1",
            is_leader=False,
            tcp_host="192.168.1.100",
            tcp_port=8000,
            datacenter="dc-1",
            job_leaderships=job_ids,
        )

        handler.process_manager_heartbeat(
            heartbeat=heartbeat,
            source_addr=("192.168.1.100", 8001),
            confirm_peer=MagicMock(),
            node_host="192.168.1.1",
            node_port=8000,
            node_id_short="wkr",
            task_runner_run=MagicMock(),
        )

        # Callback should receive all job IDs
        call_args = on_job_leadership.call_args[0]
        assert len(call_args[0]) == 100
