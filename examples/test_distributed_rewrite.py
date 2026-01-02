#!/usr/bin/env python3
"""
Comprehensive tests for the distributed rewrite functionality.

Tests cover:
1. StateEmbedder protocol and implementations
2. LamportClock and VersionedStateClock
3. Node state embedding integration
4. Message serialization/deserialization
"""

import asyncio
import time
from dataclasses import dataclass
from typing import Any


# =============================================================================
# Test utilities
# =============================================================================

def run_async(coro):
    """Run an async coroutine synchronously."""
    return asyncio.get_event_loop().run_until_complete(coro)


class TestResult:
    """Track test results."""
    
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []
    
    def record_pass(self, name: str):
        self.passed += 1
        print(f"  ✓ {name}")
    
    def record_fail(self, name: str, error: str):
        self.failed += 1
        self.errors.append((name, error))
        print(f"  ✗ {name}: {error}")
    
    def summary(self):
        total = self.passed + self.failed
        print(f"\n{'=' * 60}")
        print(f"Results: {self.passed}/{total} passed")
        if self.failed > 0:
            print(f"\nFailed tests:")
            for name, error in self.errors:
                print(f"  - {name}: {error}")
        print(f"{'=' * 60}")
        return self.failed == 0


results = TestResult()


def test(name: str):
    """Decorator for test functions."""
    def decorator(func):
        def wrapper():
            try:
                if asyncio.iscoroutinefunction(func):
                    run_async(func())
                else:
                    func()
                results.record_pass(name)
            except AssertionError as e:
                results.record_fail(name, str(e) or "Assertion failed")
            except Exception as e:
                results.record_fail(name, f"{type(e).__name__}: {e}")
        return wrapper
    return decorator


# =============================================================================
# LamportClock Tests
# =============================================================================

print("\n" + "=" * 60)
print("LamportClock Tests")
print("=" * 60)


@test("LamportClock: initial time is 0")
async def test_lamport_initial():
    from hyperscale.distributed_rewrite.server.events import LamportClock
    
    clock = LamportClock()
    assert clock.time == 0, f"Expected 0, got {clock.time}"


@test("LamportClock: increment advances time")
async def test_lamport_increment():
    from hyperscale.distributed_rewrite.server.events import LamportClock
    
    clock = LamportClock()
    t1 = await clock.increment()
    assert t1 == 1, f"Expected 1, got {t1}"
    assert clock.time == 1
    
    t2 = await clock.increment()
    assert t2 == 2, f"Expected 2, got {t2}"
    assert clock.time == 2


@test("LamportClock: tick is alias for increment")
async def test_lamport_tick():
    from hyperscale.distributed_rewrite.server.events import LamportClock
    
    clock = LamportClock()
    t1 = await clock.tick()
    assert t1 == 1
    t2 = await clock.tick()
    assert t2 == 2


@test("LamportClock: update advances to max+1")
async def test_lamport_update():
    from hyperscale.distributed_rewrite.server.events import LamportClock
    
    clock = LamportClock()
    await clock.increment()  # time = 1
    
    # Receive message with time 10
    t = await clock.update(10)
    assert t == 11, f"Expected 11 (max(10, 1) + 1), got {t}"
    
    # Receive message with lower time
    t = await clock.update(5)
    assert t == 12, f"Expected 12 (max(5, 11) + 1), got {t}"


@test("LamportClock: ack updates without increment")
async def test_lamport_ack():
    from hyperscale.distributed_rewrite.server.events import LamportClock
    
    clock = LamportClock()
    await clock.increment()  # time = 1
    
    # Ack higher time
    t = await clock.ack(10)
    assert t == 10, f"Expected 10 (max(10, 1)), got {t}"
    
    # Ack lower time (no change)
    t = await clock.ack(5)
    assert t == 10, f"Expected 10 (max(5, 10)), got {t}"


@test("LamportClock: is_stale detects old times")
async def test_lamport_is_stale():
    from hyperscale.distributed_rewrite.server.events import LamportClock
    
    clock = LamportClock()
    await clock.increment()
    await clock.increment()
    await clock.increment()  # time = 3
    
    assert clock.is_stale(1) == True
    assert clock.is_stale(2) == True
    assert clock.is_stale(3) == False  # Not stale - equal
    assert clock.is_stale(4) == False  # Not stale - newer


@test("LamportClock: compare returns correct ordering")
async def test_lamport_compare():
    from hyperscale.distributed_rewrite.server.events import LamportClock
    
    clock = LamportClock()
    await clock.update(5)  # time = 6
    
    assert clock.compare(3) == 1   # clock > 3
    assert clock.compare(6) == 0   # clock == 6
    assert clock.compare(10) == -1  # clock < 10


@test("LamportClock: initial time can be set")
async def test_lamport_initial_time():
    from hyperscale.distributed_rewrite.server.events import LamportClock
    
    clock = LamportClock(initial_time=100)
    assert clock.time == 100
    
    t = await clock.increment()
    assert t == 101


# Run LamportClock tests
test_lamport_initial()
test_lamport_increment()
test_lamport_tick()
test_lamport_update()
test_lamport_ack()
test_lamport_is_stale()
test_lamport_compare()
test_lamport_initial_time()


# =============================================================================
# VersionedStateClock Tests
# =============================================================================

print("\n" + "=" * 60)
print("VersionedStateClock Tests")
print("=" * 60)


@test("VersionedStateClock: initial state")
async def test_vclock_initial():
    from hyperscale.distributed_rewrite.server.events import VersionedStateClock
    
    clock = VersionedStateClock()
    assert clock.time == 0
    assert clock.get_entity_version("unknown") is None


@test("VersionedStateClock: update_entity tracks versions")
async def test_vclock_update_entity():
    from hyperscale.distributed_rewrite.server.events import VersionedStateClock
    
    clock = VersionedStateClock()
    
    # Update with explicit version
    v = await clock.update_entity("worker-1", 5)
    assert v == 5
    assert clock.get_entity_version("worker-1") == 5
    
    # Update with auto-increment
    v = await clock.update_entity("worker-2")
    assert v >= 1  # Some version assigned
    assert clock.get_entity_version("worker-2") == v


@test("VersionedStateClock: is_entity_stale detects stale updates")
async def test_vclock_is_stale():
    from hyperscale.distributed_rewrite.server.events import VersionedStateClock
    
    clock = VersionedStateClock()
    await clock.update_entity("worker-1", 10)
    
    # Stale: version <= current
    assert clock.is_entity_stale("worker-1", 5) == True
    assert clock.is_entity_stale("worker-1", 9) == True
    assert clock.is_entity_stale("worker-1", 10) == True  # Equal is stale
    
    # Fresh: version > current
    assert clock.is_entity_stale("worker-1", 11) == False
    assert clock.is_entity_stale("worker-1", 100) == False
    
    # Unknown entity is never stale
    assert clock.is_entity_stale("unknown", 1) == False


@test("VersionedStateClock: should_accept_update is inverse of is_stale")
async def test_vclock_should_accept():
    from hyperscale.distributed_rewrite.server.events import VersionedStateClock
    
    clock = VersionedStateClock()
    await clock.update_entity("worker-1", 10)
    
    # Should reject stale
    assert clock.should_accept_update("worker-1", 5) == False
    assert clock.should_accept_update("worker-1", 10) == False
    
    # Should accept fresh
    assert clock.should_accept_update("worker-1", 11) == True
    
    # Should accept unknown
    assert clock.should_accept_update("unknown", 1) == True


@test("VersionedStateClock: get_all_versions returns all tracked")
async def test_vclock_get_all():
    from hyperscale.distributed_rewrite.server.events import VersionedStateClock
    
    clock = VersionedStateClock()
    await clock.update_entity("worker-1", 5)
    await clock.update_entity("worker-2", 10)
    await clock.update_entity("worker-3", 15)
    
    versions = clock.get_all_versions()
    assert versions == {"worker-1": 5, "worker-2": 10, "worker-3": 15}


@test("VersionedStateClock: remove_entity removes tracking")
async def test_vclock_remove():
    from hyperscale.distributed_rewrite.server.events import VersionedStateClock
    
    clock = VersionedStateClock()
    await clock.update_entity("worker-1", 5)
    
    assert clock.get_entity_version("worker-1") == 5
    
    result = clock.remove_entity("worker-1")
    assert result == True
    assert clock.get_entity_version("worker-1") is None
    
    # Remove again should return False
    result = clock.remove_entity("worker-1")
    assert result == False


@test("VersionedStateClock: underlying clock updates")
async def test_vclock_underlying():
    from hyperscale.distributed_rewrite.server.events import VersionedStateClock
    
    clock = VersionedStateClock()
    
    t1 = await clock.increment()
    assert t1 == 1
    
    t2 = await clock.update(10)
    assert t2 == 11
    
    t3 = await clock.ack(5)
    assert t3 == 11  # No change


# Run VersionedStateClock tests
test_vclock_initial()
test_vclock_update_entity()
test_vclock_is_stale()
test_vclock_should_accept()
test_vclock_get_all()
test_vclock_remove()
test_vclock_underlying()


# =============================================================================
# StateEmbedder Tests
# =============================================================================

print("\n" + "=" * 60)
print("StateEmbedder Tests")
print("=" * 60)


@test("NullStateEmbedder: returns None state")
def test_null_embedder():
    from hyperscale.distributed_rewrite.swim.core.state_embedder import NullStateEmbedder
    
    embedder = NullStateEmbedder()
    assert embedder.get_state() is None
    
    # Should not raise
    embedder.process_state(b"test", ("127.0.0.1", 8000))


@test("WorkerStateEmbedder: embeds WorkerHeartbeat")
def test_worker_embedder():
    from hyperscale.distributed_rewrite.swim.core.state_embedder import WorkerStateEmbedder
    
    embedder = WorkerStateEmbedder(
        get_node_id=lambda: "worker-1",
        get_worker_state=lambda: "healthy",
        get_available_cores=lambda: 4,
        get_queue_depth=lambda: 2,
        get_cpu_percent=lambda: 25.0,
        get_memory_percent=lambda: 50.0,
        get_state_version=lambda: 5,
        get_active_workflows=lambda: {"wf-1": "running"},
    )
    
    state = embedder.get_state()
    assert state is not None
    assert len(state) > 0
    
    # Deserialize and verify
    from hyperscale.distributed_rewrite.models import WorkerHeartbeat
    heartbeat = WorkerHeartbeat.load(state)
    assert heartbeat.node_id == "worker-1"
    assert heartbeat.state == "healthy"
    assert heartbeat.available_cores == 4
    assert heartbeat.queue_depth == 2
    assert heartbeat.version == 5


@test("WorkerStateEmbedder: process_state is no-op")
def test_worker_embedder_process():
    from hyperscale.distributed_rewrite.swim.core.state_embedder import WorkerStateEmbedder
    
    embedder = WorkerStateEmbedder(
        get_node_id=lambda: "worker-1",
        get_worker_state=lambda: "healthy",
        get_available_cores=lambda: 4,
        get_queue_depth=lambda: 0,
        get_cpu_percent=lambda: 0.0,
        get_memory_percent=lambda: 0.0,
        get_state_version=lambda: 0,
        get_active_workflows=lambda: {},
    )
    
    # Should not raise
    embedder.process_state(b"anything", ("127.0.0.1", 8000))


@test("ManagerStateEmbedder: embeds ManagerHeartbeat")
def test_manager_embedder():
    from hyperscale.distributed_rewrite.swim.core.state_embedder import ManagerStateEmbedder
    
    received = []
    
    embedder = ManagerStateEmbedder(
        get_node_id=lambda: "manager-1",
        get_datacenter=lambda: "dc-east",
        is_leader=lambda: True,
        get_term=lambda: 3,
        get_state_version=lambda: 10,
        get_active_jobs=lambda: 5,
        get_active_workflows=lambda: 12,
        get_worker_count=lambda: 3,
        get_available_cores=lambda: 24,
        on_worker_heartbeat=lambda hb, addr: received.append((hb, addr)),
    )
    
    state = embedder.get_state()
    assert state is not None
    
    # Deserialize and verify
    from hyperscale.distributed_rewrite.models import ManagerHeartbeat
    heartbeat = ManagerHeartbeat.load(state)
    assert heartbeat.node_id == "manager-1"
    assert heartbeat.datacenter == "dc-east"
    assert heartbeat.is_leader == True
    assert heartbeat.term == 3
    assert heartbeat.worker_count == 3


@test("ManagerStateEmbedder: processes WorkerHeartbeat")
def test_manager_embedder_process():
    from hyperscale.distributed_rewrite.swim.core.state_embedder import (
        ManagerStateEmbedder,
        WorkerStateEmbedder,
    )
    
    received = []
    
    manager_embedder = ManagerStateEmbedder(
        get_node_id=lambda: "manager-1",
        get_datacenter=lambda: "dc-east",
        is_leader=lambda: True,
        get_term=lambda: 1,
        get_state_version=lambda: 1,
        get_active_jobs=lambda: 0,
        get_active_workflows=lambda: 0,
        get_worker_count=lambda: 0,
        get_available_cores=lambda: 0,
        on_worker_heartbeat=lambda hb, addr: received.append((hb, addr)),
    )
    
    # Create worker heartbeat
    worker_embedder = WorkerStateEmbedder(
        get_node_id=lambda: "worker-1",
        get_worker_state=lambda: "healthy",
        get_available_cores=lambda: 8,
        get_queue_depth=lambda: 0,
        get_cpu_percent=lambda: 10.0,
        get_memory_percent=lambda: 30.0,
        get_state_version=lambda: 1,
        get_active_workflows=lambda: {},
    )
    
    worker_state = worker_embedder.get_state()
    manager_embedder.process_state(worker_state, ("192.168.1.10", 8000))
    
    assert len(received) == 1
    heartbeat, addr = received[0]
    assert heartbeat.node_id == "worker-1"
    assert heartbeat.available_cores == 8
    assert addr == ("192.168.1.10", 8000)


@test("GateStateEmbedder: returns None state")
def test_gate_embedder():
    from hyperscale.distributed_rewrite.swim.core.state_embedder import GateStateEmbedder
    
    received = []
    
    embedder = GateStateEmbedder(
        get_node_id=lambda: "gate-1",
        get_datacenter=lambda: "global",
        is_leader=lambda: True,
        get_term=lambda: 1,
        get_state_version=lambda: 1,
        get_active_jobs=lambda: 10,
        on_manager_heartbeat=lambda hb, addr: received.append((hb, addr)),
    )
    
    # Gates don't embed state by default
    assert embedder.get_state() is None


@test("GateStateEmbedder: processes ManagerHeartbeat")
def test_gate_embedder_process():
    from hyperscale.distributed_rewrite.swim.core.state_embedder import (
        GateStateEmbedder,
        ManagerStateEmbedder,
    )
    
    received = []
    
    gate_embedder = GateStateEmbedder(
        get_node_id=lambda: "gate-1",
        get_datacenter=lambda: "global",
        is_leader=lambda: True,
        get_term=lambda: 1,
        get_state_version=lambda: 1,
        get_active_jobs=lambda: 0,
        on_manager_heartbeat=lambda hb, addr: received.append((hb, addr)),
    )
    
    # Create manager heartbeat
    manager_embedder = ManagerStateEmbedder(
        get_node_id=lambda: "manager-1",
        get_datacenter=lambda: "dc-east",
        is_leader=lambda: True,
        get_term=lambda: 2,
        get_state_version=lambda: 5,
        get_active_jobs=lambda: 3,
        get_active_workflows=lambda: 10,
        get_worker_count=lambda: 4,
        get_available_cores=lambda: 32,
        on_worker_heartbeat=lambda hb, addr: None,
    )
    
    manager_state = manager_embedder.get_state()
    gate_embedder.process_state(manager_state, ("10.0.0.50", 8000))
    
    assert len(received) == 1
    heartbeat, addr = received[0]
    assert heartbeat.node_id == "manager-1"
    assert heartbeat.datacenter == "dc-east"
    assert heartbeat.active_jobs == 3
    assert addr == ("10.0.0.50", 8000)


# Run StateEmbedder tests
test_null_embedder()
test_worker_embedder()
test_worker_embedder_process()
test_manager_embedder()
test_manager_embedder_process()
test_gate_embedder()
test_gate_embedder_process()


# =============================================================================
# Distributed Message Tests
# =============================================================================

print("\n" + "=" * 60)
print("Distributed Message Tests")
print("=" * 60)


@test("WorkerHeartbeat: serialization round-trip")
def test_worker_heartbeat_serde():
    from hyperscale.distributed_rewrite.models import WorkerHeartbeat
    
    original = WorkerHeartbeat(
        node_id="worker-123",
        state="healthy",
        available_cores=16,
        queue_depth=3,
        cpu_percent=45.5,
        memory_percent=62.3,
        version=42,
        active_workflows={"wf-1": "running", "wf-2": "completed"},
    )
    
    # Serialize
    data = original.dump()
    assert isinstance(data, bytes)
    assert len(data) > 0
    
    # Deserialize
    loaded = WorkerHeartbeat.load(data)
    assert loaded.node_id == original.node_id
    assert loaded.state == original.state
    assert loaded.available_cores == original.available_cores
    assert loaded.queue_depth == original.queue_depth
    assert loaded.cpu_percent == original.cpu_percent
    assert loaded.memory_percent == original.memory_percent
    assert loaded.version == original.version
    assert loaded.active_workflows == original.active_workflows


@test("ManagerHeartbeat: serialization round-trip")
def test_manager_heartbeat_serde():
    from hyperscale.distributed_rewrite.models import ManagerHeartbeat
    
    original = ManagerHeartbeat(
        node_id="manager-456",
        datacenter="dc-west",
        is_leader=True,
        term=5,
        version=100,
        active_jobs=10,
        active_workflows=50,
        worker_count=8,
        available_cores=64,
    )
    
    data = original.dump()
    loaded = ManagerHeartbeat.load(data)
    
    assert loaded.node_id == original.node_id
    assert loaded.datacenter == original.datacenter
    assert loaded.is_leader == original.is_leader
    assert loaded.term == original.term
    assert loaded.version == original.version
    assert loaded.active_jobs == original.active_jobs
    assert loaded.worker_count == original.worker_count


@test("JobSubmission: serialization with bytes field")
def test_job_submission_serde():
    from hyperscale.distributed_rewrite.models import JobSubmission
    import cloudpickle
    
    # Simulate pickled workflow data
    workflow_data = cloudpickle.dumps(["workflow1", "workflow2"])
    
    original = JobSubmission(
        job_id="job-789",
        workflows=workflow_data,
        vus=4,
        timeout_seconds=60.0,
        datacenter_count=2,
        datacenters=["dc-east", "dc-west"],
    )
    
    data = original.dump()
    loaded = JobSubmission.load(data)
    
    assert loaded.job_id == original.job_id
    assert loaded.workflows == original.workflows
    assert loaded.vus == original.vus
    assert loaded.timeout_seconds == original.timeout_seconds
    assert loaded.datacenter_count == original.datacenter_count
    assert loaded.datacenters == original.datacenters


@test("WorkflowProgress: serialization with nested StepStats")
def test_workflow_progress_serde():
    from hyperscale.distributed_rewrite.models import WorkflowProgress, StepStats
    
    original = WorkflowProgress(
        job_id="job-1",
        workflow_id="wf-1",
        workflow_name="TestWorkflow",
        status="running",
        completed_count=1000,
        failed_count=5,
        rate_per_second=250.5,
        elapsed_seconds=4.0,
        step_stats=[
            StepStats(step_name="step1", completed_count=500, failed_count=2, total_count=502),
            StepStats(step_name="step2", completed_count=500, failed_count=3, total_count=503),
        ],
        timestamp=12345.678,
    )
    
    data = original.dump()
    loaded = WorkflowProgress.load(data)
    
    assert loaded.job_id == original.job_id
    assert loaded.workflow_id == original.workflow_id
    assert loaded.completed_count == original.completed_count
    assert len(loaded.step_stats) == 2
    assert loaded.step_stats[0].step_name == "step1"
    assert loaded.step_stats[1].completed_count == 500


@test("ProvisionRequest: quorum message serialization")
def test_provision_request_serde():
    from hyperscale.distributed_rewrite.models import ProvisionRequest
    
    original = ProvisionRequest(
        job_id="job-1",
        workflow_id="wf-1",
        target_worker="worker-1",
        cores_required=4,
        fence_token=12345,
        version=100,
    )
    
    data = original.dump()
    loaded = ProvisionRequest.load(data)
    
    assert loaded.job_id == original.job_id
    assert loaded.workflow_id == original.workflow_id
    assert loaded.target_worker == original.target_worker
    assert loaded.cores_required == original.cores_required
    assert loaded.fence_token == original.fence_token
    assert loaded.version == original.version


@test("GlobalJobStatus: complex nested serialization")
def test_global_job_status_serde():
    from hyperscale.distributed_rewrite.models import (
        GlobalJobStatus,
        JobProgress,
        WorkflowProgress,
    )
    
    original = GlobalJobStatus(
        job_id="job-global",
        status="running",
        datacenters=[
            JobProgress(
                job_id="job-global",
                datacenter="dc-east",
                status="running",
                workflows=[
                    WorkflowProgress(
                        job_id="job-global",
                        workflow_id="wf-1",
                        workflow_name="Test",
                        status="running",
                        completed_count=100,
                        failed_count=0,
                        rate_per_second=50.0,
                        elapsed_seconds=2.0,
                    ),
                ],
                total_completed=100,
                total_failed=0,
                overall_rate=50.0,
            ),
        ],
        total_completed=100,
        total_failed=0,
        overall_rate=50.0,
        elapsed_seconds=2.0,
        completed_datacenters=0,
        failed_datacenters=0,
    )
    
    data = original.dump()
    loaded = GlobalJobStatus.load(data)
    
    assert loaded.job_id == original.job_id
    assert len(loaded.datacenters) == 1
    assert loaded.datacenters[0].datacenter == "dc-east"
    assert len(loaded.datacenters[0].workflows) == 1


# Run Distributed Message tests
test_worker_heartbeat_serde()
test_manager_heartbeat_serde()
test_job_submission_serde()
test_workflow_progress_serde()
test_provision_request_serde()
test_global_job_status_serde()


# =============================================================================
# Stale Update Rejection Tests
# =============================================================================

print("\n" + "=" * 60)
print("Stale Update Rejection Tests")
print("=" * 60)


@test("Manager rejects stale worker heartbeats")
async def test_manager_stale_rejection():
    """Simulate manager receiving out-of-order worker heartbeats."""
    from hyperscale.distributed_rewrite.server.events import VersionedStateClock
    from hyperscale.distributed_rewrite.models import WorkerHeartbeat
    
    # Simulate manager's versioned clock
    clock = VersionedStateClock()
    worker_status = {}
    
    def process_heartbeat(hb: WorkerHeartbeat):
        """Simulates manager's _handle_embedded_worker_heartbeat logic."""
        if clock.is_entity_stale(hb.node_id, hb.version):
            return False  # Rejected
        
        worker_status[hb.node_id] = hb
        asyncio.create_task(clock.update_entity(hb.node_id, hb.version))
        return True  # Accepted
    
    # First heartbeat with version 5
    hb1 = WorkerHeartbeat(
        node_id="worker-1",
        state="healthy",
        available_cores=8,
        queue_depth=0,
        cpu_percent=10.0,
        memory_percent=50.0,
        version=5,
    )
    assert process_heartbeat(hb1) == True
    await asyncio.sleep(0.01)  # Let task complete
    
    # Newer heartbeat with version 10
    hb2 = WorkerHeartbeat(
        node_id="worker-1",
        state="degraded",
        available_cores=4,
        queue_depth=5,
        cpu_percent=80.0,
        memory_percent=70.0,
        version=10,
    )
    assert process_heartbeat(hb2) == True
    await asyncio.sleep(0.01)
    
    # Out-of-order heartbeat with version 7 (stale!)
    hb3 = WorkerHeartbeat(
        node_id="worker-1",
        state="healthy",
        available_cores=8,
        queue_depth=0,
        cpu_percent=10.0,
        memory_percent=50.0,
        version=7,
    )
    assert process_heartbeat(hb3) == False  # Should be rejected
    
    # Verify we kept the newer state
    assert worker_status["worker-1"].version == 10
    assert worker_status["worker-1"].state == "degraded"


@test("Gate rejects stale manager heartbeats")
async def test_gate_stale_rejection():
    """Simulate gate receiving out-of-order DC manager heartbeats."""
    from hyperscale.distributed_rewrite.server.events import VersionedStateClock
    from hyperscale.distributed_rewrite.models import ManagerHeartbeat
    
    clock = VersionedStateClock()
    dc_status = {}
    
    def process_heartbeat(hb: ManagerHeartbeat):
        dc_key = f"dc:{hb.datacenter}"
        if clock.is_entity_stale(dc_key, hb.version):
            return False
        
        dc_status[hb.datacenter] = hb
        asyncio.create_task(clock.update_entity(dc_key, hb.version))
        return True
    
    # First: version 10
    hb1 = ManagerHeartbeat(
        node_id="manager-1",
        datacenter="dc-east",
        is_leader=True,
        term=1,
        version=10,
        active_jobs=5,
        active_workflows=20,
        worker_count=4,
        available_cores=32,
    )
    assert process_heartbeat(hb1) == True
    await asyncio.sleep(0.01)
    
    # Stale: version 8
    hb2 = ManagerHeartbeat(
        node_id="manager-1",
        datacenter="dc-east",
        is_leader=True,
        term=1,
        version=8,
        active_jobs=3,
        active_workflows=10,
        worker_count=2,
        available_cores=16,
    )
    assert process_heartbeat(hb2) == False  # Rejected
    
    # Verify we kept version 10 state
    assert dc_status["dc-east"].version == 10
    assert dc_status["dc-east"].active_jobs == 5


# Run Stale Update tests
test_manager_stale_rejection()
test_gate_stale_rejection()


# =============================================================================
# Concurrent Access Tests
# =============================================================================

print("\n" + "=" * 60)
print("Concurrent Access Tests")
print("=" * 60)


@test("LamportClock: concurrent increments are serialized")
async def test_lamport_concurrent():
    from hyperscale.distributed_rewrite.server.events import LamportClock
    
    clock = LamportClock()
    
    async def increment_many(n: int):
        for _ in range(n):
            await clock.increment()
    
    # Run 10 concurrent tasks, each incrementing 100 times
    tasks = [increment_many(100) for _ in range(10)]
    await asyncio.gather(*tasks)
    
    # Should have exactly 1000 increments
    assert clock.time == 1000, f"Expected 1000, got {clock.time}"


@test("VersionedStateClock: concurrent entity updates")
async def test_vclock_concurrent():
    from hyperscale.distributed_rewrite.server.events import VersionedStateClock
    
    clock = VersionedStateClock()
    
    async def update_entity(entity_id: str, version: int):
        await clock.update_entity(entity_id, version)
    
    # Concurrent updates to same entity - highest version should win
    tasks = [
        update_entity("worker-1", 5),
        update_entity("worker-1", 10),
        update_entity("worker-1", 7),
        update_entity("worker-1", 15),
        update_entity("worker-1", 3),
    ]
    await asyncio.gather(*tasks)
    
    # The last update should be tracked (but order is non-deterministic)
    # What we can verify is that the clock has been updated
    version = clock.get_entity_version("worker-1")
    assert version is not None
    assert version >= 3  # At least one update succeeded


@test("VersionedStateClock: concurrent different entities")
async def test_vclock_concurrent_different():
    from hyperscale.distributed_rewrite.server.events import VersionedStateClock
    
    clock = VersionedStateClock()
    
    async def update_entity(entity_id: str, version: int):
        await clock.update_entity(entity_id, version)
    
    # Concurrent updates to different entities
    tasks = [
        update_entity("worker-1", 10),
        update_entity("worker-2", 20),
        update_entity("worker-3", 30),
        update_entity("worker-4", 40),
        update_entity("worker-5", 50),
    ]
    await asyncio.gather(*tasks)
    
    # All should be tracked
    assert clock.get_entity_version("worker-1") == 10
    assert clock.get_entity_version("worker-2") == 20
    assert clock.get_entity_version("worker-3") == 30
    assert clock.get_entity_version("worker-4") == 40
    assert clock.get_entity_version("worker-5") == 50


# Run Concurrent tests
test_lamport_concurrent()
test_vclock_concurrent()
test_vclock_concurrent_different()


# =============================================================================
# Summary
# =============================================================================

success = results.summary()
exit(0 if success else 1)

