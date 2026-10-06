"""
Unit tests for Manager Models from Section 15.4.2 of REFACTOR.md.

Tests cover:
- PeerState and GatePeerState
- WorkerSyncState
- JobSyncState

Each test class validates:
- Happy path (normal operations)
- Negative path (invalid inputs, error conditions)
- Failure modes (exception handling)
- Concurrency and race conditions
- Edge cases (boundary conditions, special values)
"""

import asyncio
import pytest
import time

from hyperscale.distributed.nodes.manager.models import (
    PeerState,
    GatePeerState,
    WorkerSyncState,
    JobSyncState,
)


# =============================================================================
# PeerState Tests
# =============================================================================


class TestPeerStateHappyPath:
    """Happy path tests for PeerState."""

    def test_create_with_required_fields(self):
        """Create PeerState with all required fields."""
        state = PeerState(
            node_id="manager-123",
            tcp_host="192.168.1.10",
            tcp_port=8000,
            udp_host="192.168.1.10",
            udp_port=8001,
            datacenter_id="dc-east",
        )

        assert state.node_id == "manager-123"
        assert state.tcp_host == "192.168.1.10"
        assert state.tcp_port == 8000
        assert state.udp_host == "192.168.1.10"
        assert state.udp_port == 8001
        assert state.datacenter_id == "dc-east"

    def test_default_optional_fields(self):
        """Check default values for optional fields."""
        state = PeerState(
            node_id="manager-456",
            tcp_host="10.0.0.1",
            tcp_port=9000,
            udp_host="10.0.0.1",
            udp_port=9001,
            datacenter_id="dc-west",
        )

        assert state.is_leader is False
        assert state.term == 0
        assert state.state_version == 0
        assert state.last_seen == 0.0
        assert state.is_active is False
        assert state.epoch == 0

    def test_tcp_addr_property(self):
        """tcp_addr property returns correct tuple."""
        state = PeerState(
            node_id="manager-789",
            tcp_host="127.0.0.1",
            tcp_port=5000,
            udp_host="127.0.0.1",
            udp_port=5001,
            datacenter_id="dc-local",
        )

        assert state.tcp_addr == ("127.0.0.1", 5000)

    def test_udp_addr_property(self):
        """udp_addr property returns correct tuple."""
        state = PeerState(
            node_id="manager-abc",
            tcp_host="10.1.1.1",
            tcp_port=6000,
            udp_host="10.1.1.1",
            udp_port=6001,
            datacenter_id="dc-central",
        )

        assert state.udp_addr == ("10.1.1.1", 6001)

    def test_leader_state(self):
        """PeerState can track leader status."""
        state = PeerState(
            node_id="manager-leader",
            tcp_host="10.0.0.1",
            tcp_port=8000,
            udp_host="10.0.0.1",
            udp_port=8001,
            datacenter_id="dc-east",
            is_leader=True,
            term=5,
        )

        assert state.is_leader is True
        assert state.term == 5


class TestPeerStateNegativePath:
    """Negative path tests for PeerState."""

    def test_missing_required_fields_raises_type_error(self):
        """Missing required fields should raise TypeError."""
        with pytest.raises(TypeError):
            PeerState()

        with pytest.raises(TypeError):
            PeerState(node_id="manager-123")

    def test_slots_prevents_arbitrary_attributes(self):
        """slots=True prevents adding arbitrary attributes."""
        state = PeerState(
            node_id="manager-slots",
            tcp_host="10.0.0.1",
            tcp_port=8000,
            udp_host="10.0.0.1",
            udp_port=8001,
            datacenter_id="dc-east",
        )

        with pytest.raises(AttributeError):
            state.arbitrary_field = "value"


class TestPeerStateEdgeCases:
    """Edge case tests for PeerState."""

    def test_empty_node_id(self):
        """Empty node_id should be allowed."""
        state = PeerState(
            node_id="",
            tcp_host="10.0.0.1",
            tcp_port=8000,
            udp_host="10.0.0.1",
            udp_port=8001,
            datacenter_id="dc-east",
        )
        assert state.node_id == ""

    def test_very_long_node_id(self):
        """Very long node_id should be handled."""
        long_id = "m" * 10000
        state = PeerState(
            node_id=long_id,
            tcp_host="10.0.0.1",
            tcp_port=8000,
            udp_host="10.0.0.1",
            udp_port=8001,
            datacenter_id="dc-east",
        )
        assert len(state.node_id) == 10000

    def test_special_characters_in_datacenter_id(self):
        """Special characters in datacenter_id should work."""
        special_ids = ["dc-east-1", "dc_west_2", "dc.central.3", "dc:asia:pacific"]
        for dc_id in special_ids:
            state = PeerState(
                node_id="manager-123",
                tcp_host="10.0.0.1",
                tcp_port=8000,
                udp_host="10.0.0.1",
                udp_port=8001,
                datacenter_id=dc_id,
            )
            assert state.datacenter_id == dc_id

    def test_maximum_port_number(self):
        """Maximum port number (65535) should work."""
        state = PeerState(
            node_id="manager-123",
            tcp_host="10.0.0.1",
            tcp_port=65535,
            udp_host="10.0.0.1",
            udp_port=65535,
            datacenter_id="dc-east",
        )
        assert state.tcp_port == 65535
        assert state.udp_port == 65535

    def test_zero_port_number(self):
        """Zero port number should be allowed (though not practical)."""
        state = PeerState(
            node_id="manager-123",
            tcp_host="10.0.0.1",
            tcp_port=0,
            udp_host="10.0.0.1",
            udp_port=0,
            datacenter_id="dc-east",
        )
        assert state.tcp_port == 0
        assert state.udp_port == 0

    def test_ipv6_host(self):
        """IPv6 addresses should work."""
        state = PeerState(
            node_id="manager-ipv6",
            tcp_host="::1",
            tcp_port=8000,
            udp_host="2001:db8::1",
            udp_port=8001,
            datacenter_id="dc-east",
        )
        assert state.tcp_host == "::1"
        assert state.udp_host == "2001:db8::1"

    def test_hostname_instead_of_ip(self):
        """Hostnames should work as well as IPs."""
        state = PeerState(
            node_id="manager-hostname",
            tcp_host="manager-1.example.com",
            tcp_port=8000,
            udp_host="manager-1.example.com",
            udp_port=8001,
            datacenter_id="dc-east",
        )
        assert state.tcp_host == "manager-1.example.com"

    def test_very_large_term_and_epoch(self):
        """Very large term and epoch values should work."""
        state = PeerState(
            node_id="manager-large-values",
            tcp_host="10.0.0.1",
            tcp_port=8000,
            udp_host="10.0.0.1",
            udp_port=8001,
            datacenter_id="dc-east",
            term=2**63 - 1,
            epoch=2**63 - 1,
        )
        assert state.term == 2**63 - 1
        assert state.epoch == 2**63 - 1


class TestPeerStateConcurrency:
    """Concurrency tests for PeerState."""

    @pytest.mark.asyncio
    async def test_multiple_peer_states_independent(self):
        """Multiple PeerState instances should be independent."""
        states = [
            PeerState(
                node_id=f"manager-{i}",
                tcp_host=f"10.0.0.{i}",
                tcp_port=8000 + i,
                udp_host=f"10.0.0.{i}",
                udp_port=9000 + i,
                datacenter_id="dc-east",
            )
            for i in range(100)
        ]

        # All states should be independent
        assert len(set(s.node_id for s in states)) == 100
        assert len(set(s.tcp_port for s in states)) == 100


# =============================================================================
# GatePeerState Tests
# =============================================================================


class TestGatePeerStateHappyPath:
    """Happy path tests for GatePeerState."""

    def test_create_with_required_fields(self):
        """Create GatePeerState with all required fields."""
        state = GatePeerState(
            node_id="gate-123",
            tcp_host="192.168.1.20",
            tcp_port=7000,
            udp_host="192.168.1.20",
            udp_port=7001,
            datacenter_id="dc-east",
        )

        assert state.node_id == "gate-123"
        assert state.tcp_host == "192.168.1.20"
        assert state.tcp_port == 7000

    def test_default_optional_fields(self):
        """Check default values for optional fields."""
        state = GatePeerState(
            node_id="gate-456",
            tcp_host="10.0.0.2",
            tcp_port=7000,
            udp_host="10.0.0.2",
            udp_port=7001,
            datacenter_id="dc-west",
        )

        assert state.is_leader is False
        assert state.is_healthy is True
        assert state.last_seen == 0.0
        assert state.epoch == 0

    def test_tcp_and_udp_addr_properties(self):
        """tcp_addr and udp_addr properties return correct tuples."""
        state = GatePeerState(
            node_id="gate-789",
            tcp_host="127.0.0.1",
            tcp_port=5000,
            udp_host="127.0.0.1",
            udp_port=5001,
            datacenter_id="dc-local",
        )

        assert state.tcp_addr == ("127.0.0.1", 5000)
        assert state.udp_addr == ("127.0.0.1", 5001)


class TestGatePeerStateEdgeCases:
    """Edge case tests for GatePeerState."""

    def test_unhealthy_gate(self):
        """Gate can be marked as unhealthy."""
        state = GatePeerState(
            node_id="gate-unhealthy",
            tcp_host="10.0.0.1",
            tcp_port=7000,
            udp_host="10.0.0.1",
            udp_port=7001,
            datacenter_id="dc-east",
            is_healthy=False,
        )

        assert state.is_healthy is False

    def test_slots_prevents_arbitrary_attributes(self):
        """slots=True prevents adding arbitrary attributes."""
        state = GatePeerState(
            node_id="gate-slots",
            tcp_host="10.0.0.1",
            tcp_port=7000,
            udp_host="10.0.0.1",
            udp_port=7001,
            datacenter_id="dc-east",
        )

        with pytest.raises(AttributeError):
            state.new_field = "value"


# =============================================================================
# WorkerSyncState Tests
# =============================================================================


class TestWorkerSyncStateHappyPath:
    """Happy path tests for WorkerSyncState."""

    def test_create_with_required_fields(self):
        """Create WorkerSyncState with required fields."""
        state = WorkerSyncState(
            worker_id="worker-123",
            tcp_host="192.168.1.30",
            tcp_port=6000,
        )

        assert state.worker_id == "worker-123"
        assert state.tcp_host == "192.168.1.30"
        assert state.tcp_port == 6000

    def test_default_optional_fields(self):
        """Check default values for optional fields."""
        state = WorkerSyncState(
            worker_id="worker-456",
            tcp_host="10.0.0.3",
            tcp_port=6000,
        )

        assert state.sync_requested_at == 0.0
        assert state.sync_completed_at is None
        assert state.sync_success is False
        assert state.sync_attempts == 0
        assert state.last_error is None

    def test_tcp_addr_property(self):
        """tcp_addr property returns correct tuple."""
        state = WorkerSyncState(
            worker_id="worker-789",
            tcp_host="127.0.0.1",
            tcp_port=4000,
        )

        assert state.tcp_addr == ("127.0.0.1", 4000)

    def test_is_synced_property_false_when_not_synced(self):
        """is_synced is False when sync not complete."""
        state = WorkerSyncState(
            worker_id="worker-not-synced",
            tcp_host="10.0.0.1",
            tcp_port=6000,
        )

        assert state.is_synced is False

    def test_is_synced_property_true_when_synced(self):
        """is_synced is True when sync succeeded."""
        state = WorkerSyncState(
            worker_id="worker-synced",
            tcp_host="10.0.0.1",
            tcp_port=6000,
            sync_success=True,
            sync_completed_at=time.monotonic(),
        )

        assert state.is_synced is True


class TestWorkerSyncStateEdgeCases:
    """Edge case tests for WorkerSyncState."""

    def test_sync_failure_with_error(self):
        """Can track sync failure with error message."""
        state = WorkerSyncState(
            worker_id="worker-failed",
            tcp_host="10.0.0.1",
            tcp_port=6000,
            sync_success=False,
            sync_attempts=3,
            last_error="Connection refused",
        )

        assert state.sync_success is False
        assert state.sync_attempts == 3
        assert state.last_error == "Connection refused"

    def test_many_sync_attempts(self):
        """Can track many sync attempts."""
        state = WorkerSyncState(
            worker_id="worker-many-attempts",
            tcp_host="10.0.0.1",
            tcp_port=6000,
            sync_attempts=1000,
        )

        assert state.sync_attempts == 1000

    def test_sync_completed_but_not_successful(self):
        """sync_completed_at set but sync_success False."""
        state = WorkerSyncState(
            worker_id="worker-completed-failed",
            tcp_host="10.0.0.1",
            tcp_port=6000,
            sync_success=False,
            sync_completed_at=time.monotonic(),
        )

        # Not synced because sync_success is False
        assert state.is_synced is False


# =============================================================================
# JobSyncState Tests
# =============================================================================


class TestJobSyncStateHappyPath:
    """Happy path tests for JobSyncState."""

    def test_create_with_required_fields(self):
        """Create JobSyncState with required field."""
        state = JobSyncState(job_id="job-123")

        assert state.job_id == "job-123"

    def test_default_optional_fields(self):
        """Check default values for optional fields."""
        state = JobSyncState(job_id="job-456")

        assert state.leader_node_id is None
        assert state.fencing_token == 0
        assert state.layer_version == 0
        assert state.workflow_count == 0
        assert state.completed_count == 0
        assert state.failed_count == 0
        assert state.sync_source is None
        assert state.sync_timestamp == 0.0

    def test_is_complete_property_false_when_incomplete(self):
        """is_complete is False when workflows still pending."""
        state = JobSyncState(
            job_id="job-incomplete",
            workflow_count=10,
            completed_count=5,
            failed_count=2,
        )

        assert state.is_complete is False

    def test_is_complete_property_true_when_all_finished(self):
        """is_complete is True when all workflows finished."""
        state = JobSyncState(
            job_id="job-complete",
            workflow_count=10,
            completed_count=8,
            failed_count=2,
        )

        assert state.is_complete is True

    def test_is_complete_all_successful(self):
        """is_complete is True with all successful completions."""
        state = JobSyncState(
            job_id="job-all-success",
            workflow_count=10,
            completed_count=10,
            failed_count=0,
        )

        assert state.is_complete is True


class TestJobSyncStateEdgeCases:
    """Edge case tests for JobSyncState."""

    def test_zero_workflows(self):
        """Job with zero workflows is considered complete."""
        state = JobSyncState(
            job_id="job-empty",
            workflow_count=0,
            completed_count=0,
            failed_count=0,
        )

        assert state.is_complete is True

    def test_more_finished_than_total(self):
        """Edge case: more finished than total (shouldn't happen but handle gracefully)."""
        state = JobSyncState(
            job_id="job-overflow",
            workflow_count=5,
            completed_count=10,  # More than workflow_count
            failed_count=0,
        )

        # Still considered complete
        assert state.is_complete is True

    def test_large_workflow_counts(self):
        """Large workflow counts should work."""
        state = JobSyncState(
            job_id="job-large",
            workflow_count=1_000_000,
            completed_count=999_999,
            failed_count=0,
        )

        assert state.is_complete is False
        assert state.workflow_count == 1_000_000


# =============================================================================
# Cross-Model Tests
# =============================================================================


class TestAllModelsUseSlots:
    """Verify all models use slots=True for memory efficiency."""

    def test_peer_state_uses_slots(self):
        """PeerState uses slots."""
        state = PeerState(
            node_id="m", tcp_host="h", tcp_port=1,
            udp_host="h", udp_port=2, datacenter_id="d"
        )
        with pytest.raises(AttributeError):
            state.new_attr = "x"

    def test_gate_peer_state_uses_slots(self):
        """GatePeerState uses slots."""
        state = GatePeerState(
            node_id="g", tcp_host="h", tcp_port=1,
            udp_host="h", udp_port=2, datacenter_id="d"
        )
        with pytest.raises(AttributeError):
            state.new_attr = "x"

    def test_worker_sync_state_uses_slots(self):
        """WorkerSyncState uses slots."""
        state = WorkerSyncState(worker_id="w", tcp_host="h", tcp_port=1)
        with pytest.raises(AttributeError):
            state.new_attr = "x"

    def test_job_sync_state_uses_slots(self):
        """JobSyncState uses slots."""
        state = JobSyncState(job_id="j")
        with pytest.raises(AttributeError):
            state.new_attr = "x"
