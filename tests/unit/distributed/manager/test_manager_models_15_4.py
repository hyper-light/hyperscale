"""
Unit tests for Manager Models from Section 15.4.2 of REFACTOR.md.

Tests cover:
- PeerState and GatePeerState
- WorkerSyncState
- JobSyncState
- WorkflowLifecycleState
- ProvisionState

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
    WorkflowLifecycleState,
    ProvisionState,
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
# WorkflowLifecycleState Tests
# =============================================================================


class TestWorkflowLifecycleStateHappyPath:
    """Happy path tests for WorkflowLifecycleState."""

    def test_create_with_required_fields(self):
        """Create WorkflowLifecycleState with required fields."""
        state = WorkflowLifecycleState(
            workflow_id="workflow-123",
            job_id="job-456",
        )

        assert state.workflow_id == "workflow-123"
        assert state.job_id == "job-456"

    def test_default_optional_fields(self):
        """Check default values for optional fields."""
        state = WorkflowLifecycleState(
            workflow_id="workflow-789",
            job_id="job-abc",
        )

        assert state.worker_id is None
        assert state.fence_token == 0
        assert state.retry_count == 0
        assert state.max_retries == 3
        assert state.dispatch_timestamp == 0.0
        assert state.last_progress_timestamp == 0.0
        assert state.failed_workers == frozenset()

    def test_can_retry_property_true(self):
        """can_retry is True when retries available."""
        state = WorkflowLifecycleState(
            workflow_id="workflow-retry",
            job_id="job-retry",
            retry_count=1,
            max_retries=3,
        )

        assert state.can_retry is True

    def test_can_retry_property_false(self):
        """can_retry is False when max retries reached."""
        state = WorkflowLifecycleState(
            workflow_id="workflow-no-retry",
            job_id="job-no-retry",
            retry_count=3,
            max_retries=3,
        )

        assert state.can_retry is False


class TestWorkflowLifecycleStateRecordFailure:
    """Tests for record_failure method."""

    def test_record_failure_creates_new_state(self):
        """record_failure returns new state, doesn't mutate original."""
        original = WorkflowLifecycleState(
            workflow_id="workflow-fail",
            job_id="job-fail",
            worker_id="worker-1",
            retry_count=0,
        )

        new_state = original.record_failure("worker-1")

        # Original unchanged
        assert original.retry_count == 0
        assert original.worker_id == "worker-1"
        assert original.failed_workers == frozenset()

        # New state updated
        assert new_state.retry_count == 1
        assert new_state.worker_id is None
        assert new_state.failed_workers == frozenset({"worker-1"})

    def test_record_failure_accumulates_workers(self):
        """Multiple failures accumulate failed workers."""
        state = WorkflowLifecycleState(
            workflow_id="workflow-multi-fail",
            job_id="job-multi-fail",
            failed_workers=frozenset({"worker-1"}),
            retry_count=1,
        )

        new_state = state.record_failure("worker-2")

        assert new_state.failed_workers == frozenset({"worker-1", "worker-2"})
        assert new_state.retry_count == 2

    def test_record_failure_preserves_other_fields(self):
        """record_failure preserves other fields."""
        original = WorkflowLifecycleState(
            workflow_id="workflow-preserve",
            job_id="job-preserve",
            fence_token=5,
            max_retries=5,
            dispatch_timestamp=100.0,
            last_progress_timestamp=150.0,
        )

        new_state = original.record_failure("worker-1")

        assert new_state.workflow_id == "workflow-preserve"
        assert new_state.job_id == "job-preserve"
        assert new_state.fence_token == 5
        assert new_state.max_retries == 5
        assert new_state.dispatch_timestamp == 100.0
        assert new_state.last_progress_timestamp == 150.0


class TestWorkflowLifecycleStateEdgeCases:
    """Edge case tests for WorkflowLifecycleState."""

    def test_zero_max_retries(self):
        """Zero max_retries means no retries allowed."""
        state = WorkflowLifecycleState(
            workflow_id="workflow-no-retries",
            job_id="job-no-retries",
            max_retries=0,
        )

        assert state.can_retry is False

    def test_many_failed_workers(self):
        """Can track many failed workers."""
        failed = frozenset(f"worker-{i}" for i in range(100))
        state = WorkflowLifecycleState(
            workflow_id="workflow-many-fails",
            job_id="job-many-fails",
            failed_workers=failed,
        )

        assert len(state.failed_workers) == 100

    def test_slots_prevents_arbitrary_attributes(self):
        """slots=True prevents adding arbitrary attributes."""
        state = WorkflowLifecycleState(
            workflow_id="workflow-slots",
            job_id="job-slots",
        )

        with pytest.raises(AttributeError):
            state.extra_field = "value"


# =============================================================================
# ProvisionState Tests
# =============================================================================


class TestProvisionStateHappyPath:
    """Happy path tests for ProvisionState."""

    def test_create_with_required_fields(self):
        """Create ProvisionState with required fields."""
        state = ProvisionState(
            workflow_id="workflow-prov-123",
            job_id="job-prov-456",
            worker_id="worker-prov-789",
            cores_requested=4,
        )

        assert state.workflow_id == "workflow-prov-123"
        assert state.job_id == "job-prov-456"
        assert state.worker_id == "worker-prov-789"
        assert state.cores_requested == 4

    def test_default_optional_fields(self):
        """Check default values for optional fields."""
        state = ProvisionState(
            workflow_id="workflow-defaults",
            job_id="job-defaults",
            worker_id="worker-defaults",
            cores_requested=2,
        )

        assert state.initiated_at > 0  # Set by default_factory
        assert state.confirmed_nodes == frozenset()
        assert state.timeout_seconds == 5.0

    def test_confirmation_count_property(self):
        """confirmation_count returns correct count."""
        state = ProvisionState(
            workflow_id="workflow-count",
            job_id="job-count",
            worker_id="worker-count",
            cores_requested=1,
            confirmed_nodes=frozenset({"node-1", "node-2", "node-3"}),
        )

        assert state.confirmation_count == 3


class TestProvisionStateAddConfirmation:
    """Tests for add_confirmation method."""

    def test_add_confirmation_creates_new_state(self):
        """add_confirmation returns new state, doesn't mutate original."""
        original = ProvisionState(
            workflow_id="workflow-confirm",
            job_id="job-confirm",
            worker_id="worker-confirm",
            cores_requested=2,
        )

        new_state = original.add_confirmation("node-1")

        # Original unchanged
        assert original.confirmed_nodes == frozenset()

        # New state updated
        assert new_state.confirmed_nodes == frozenset({"node-1"})

    def test_add_confirmation_accumulates(self):
        """Multiple confirmations accumulate."""
        state = ProvisionState(
            workflow_id="workflow-multi-confirm",
            job_id="job-multi-confirm",
            worker_id="worker-multi-confirm",
            cores_requested=2,
            confirmed_nodes=frozenset({"node-1"}),
        )

        state2 = state.add_confirmation("node-2")
        state3 = state2.add_confirmation("node-3")

        assert state3.confirmed_nodes == frozenset({"node-1", "node-2", "node-3"})

    def test_add_confirmation_preserves_fields(self):
        """add_confirmation preserves other fields."""
        initiated = 100.0
        original = ProvisionState(
            workflow_id="workflow-preserve",
            job_id="job-preserve",
            worker_id="worker-preserve",
            cores_requested=8,
            initiated_at=initiated,
            timeout_seconds=10.0,
        )

        new_state = original.add_confirmation("node-1")

        assert new_state.workflow_id == "workflow-preserve"
        assert new_state.job_id == "job-preserve"
        assert new_state.worker_id == "worker-preserve"
        assert new_state.cores_requested == 8
        assert new_state.initiated_at == initiated
        assert new_state.timeout_seconds == 10.0


class TestProvisionStateHasQuorum:
    """Tests for has_quorum method."""

    def test_has_quorum_true_when_enough_confirmations(self):
        """has_quorum is True when confirmations >= quorum_size."""
        state = ProvisionState(
            workflow_id="workflow-quorum",
            job_id="job-quorum",
            worker_id="worker-quorum",
            cores_requested=1,
            confirmed_nodes=frozenset({"node-1", "node-2", "node-3"}),
        )

        assert state.has_quorum(3) is True
        assert state.has_quorum(2) is True

    def test_has_quorum_false_when_not_enough(self):
        """has_quorum is False when confirmations < quorum_size."""
        state = ProvisionState(
            workflow_id="workflow-no-quorum",
            job_id="job-no-quorum",
            worker_id="worker-no-quorum",
            cores_requested=1,
            confirmed_nodes=frozenset({"node-1"}),
        )

        assert state.has_quorum(3) is False


class TestProvisionStateIsTimedOut:
    """Tests for is_timed_out property."""

    def test_is_timed_out_false_when_fresh(self):
        """is_timed_out is False for fresh provision."""
        state = ProvisionState(
            workflow_id="workflow-fresh",
            job_id="job-fresh",
            worker_id="worker-fresh",
            cores_requested=1,
            timeout_seconds=5.0,
        )

        assert state.is_timed_out is False

    def test_is_timed_out_true_after_timeout(self):
        """is_timed_out is True after timeout elapsed."""
        # Create state with initiated_at in the past
        old_time = time.monotonic() - 10.0  # 10 seconds ago
        state = ProvisionState(
            workflow_id="workflow-old",
            job_id="job-old",
            worker_id="worker-old",
            cores_requested=1,
            initiated_at=old_time,
            timeout_seconds=5.0,  # 5 second timeout
        )

        assert state.is_timed_out is True


class TestProvisionStateEdgeCases:
    """Edge case tests for ProvisionState."""

    def test_zero_cores_requested(self):
        """Zero cores requested should work (though unusual)."""
        state = ProvisionState(
            workflow_id="workflow-zero-cores",
            job_id="job-zero-cores",
            worker_id="worker-zero-cores",
            cores_requested=0,
        )

        assert state.cores_requested == 0

    def test_very_short_timeout(self):
        """Very short timeout should work."""
        state = ProvisionState(
            workflow_id="workflow-short-timeout",
            job_id="job-short-timeout",
            worker_id="worker-short-timeout",
            cores_requested=1,
            timeout_seconds=0.001,
        )

        # Should be timed out almost immediately
        import time
        time.sleep(0.01)
        assert state.is_timed_out is True

    def test_zero_timeout(self):
        """Zero timeout means always timed out."""
        state = ProvisionState(
            workflow_id="workflow-zero-timeout",
            job_id="job-zero-timeout",
            worker_id="worker-zero-timeout",
            cores_requested=1,
            timeout_seconds=0.0,
        )

        assert state.is_timed_out is True

    def test_quorum_size_one(self):
        """Single-node quorum should work."""
        state = ProvisionState(
            workflow_id="workflow-single",
            job_id="job-single",
            worker_id="worker-single",
            cores_requested=1,
            confirmed_nodes=frozenset({"node-1"}),
        )

        assert state.has_quorum(1) is True

    def test_quorum_size_zero(self):
        """Zero quorum size should always succeed."""
        state = ProvisionState(
            workflow_id="workflow-zero-quorum",
            job_id="job-zero-quorum",
            worker_id="worker-zero-quorum",
            cores_requested=1,
        )

        assert state.has_quorum(0) is True

    def test_duplicate_confirmation(self):
        """Adding same node twice doesn't increase count."""
        state = ProvisionState(
            workflow_id="workflow-dup",
            job_id="job-dup",
            worker_id="worker-dup",
            cores_requested=1,
            confirmed_nodes=frozenset({"node-1"}),
        )

        new_state = state.add_confirmation("node-1")

        # Still only 1 confirmation (frozenset deduplicates)
        assert new_state.confirmation_count == 1


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

    def test_workflow_lifecycle_state_uses_slots(self):
        """WorkflowLifecycleState uses slots."""
        state = WorkflowLifecycleState(workflow_id="w", job_id="j")
        with pytest.raises(AttributeError):
            state.new_attr = "x"

    def test_provision_state_uses_slots(self):
        """ProvisionState uses slots."""
        state = ProvisionState(
            workflow_id="w", job_id="j", worker_id="w", cores_requested=1
        )
        with pytest.raises(AttributeError):
            state.new_attr = "x"
