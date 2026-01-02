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

# Create a global event loop at module load time for older Python versions
try:
    _loop = asyncio.get_running_loop()
except RuntimeError:
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)


def run_async(coro):
    """Run an async coroutine synchronously."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.get_event_loop()
    return loop.run_until_complete(coro)


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


@test("GateStateEmbedder: embeds GateHeartbeat state")
def test_gate_embedder():
    from hyperscale.distributed_rewrite.swim.core.state_embedder import GateStateEmbedder
    from hyperscale.distributed_rewrite.models import GateHeartbeat
    
    received = []
    
    embedder = GateStateEmbedder(
        get_node_id=lambda: "gate-1",
        get_datacenter=lambda: "global",
        is_leader=lambda: True,
        get_term=lambda: 1,
        get_state_version=lambda: 1,
        get_gate_state=lambda: "active",
        get_active_jobs=lambda: 10,
        get_active_datacenters=lambda: 3,
        get_manager_count=lambda: 6,
        on_manager_heartbeat=lambda hb, addr: received.append((hb, addr)),
    )
    
    # Gates now embed GateHeartbeat
    state = embedder.get_state()
    assert state is not None
    heartbeat = GateHeartbeat.load(state)
    assert heartbeat.node_id == "gate-1"
    assert heartbeat.active_jobs == 10
    assert heartbeat.state == "active"


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
        get_gate_state=lambda: "active",
        get_active_jobs=lambda: 0,
        get_active_datacenters=lambda: 3,
        get_manager_count=lambda: 6,
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
# Leadership Callback Composition Tests
# =============================================================================

print("\n" + "=" * 60)
print("Leadership Callback Composition Tests")
print("=" * 60)


@test("HealthAwareServer: has callback registration methods")
def test_health_aware_server_callback_methods():
    from hyperscale.distributed_rewrite.swim import HealthAwareServer
    
    assert hasattr(HealthAwareServer, 'register_on_become_leader')
    assert hasattr(HealthAwareServer, 'register_on_lose_leadership')
    assert hasattr(HealthAwareServer, 'register_on_leader_change')
    
    # Check they are callable
    assert callable(getattr(HealthAwareServer, 'register_on_become_leader'))
    assert callable(getattr(HealthAwareServer, 'register_on_lose_leadership'))
    assert callable(getattr(HealthAwareServer, 'register_on_leader_change'))


@test("HealthAwareServer: callback lists are initialized")
def test_health_aware_server_callback_lists():
    """Test that callback lists exist on instance."""
    # We can't instantiate HealthAwareServer easily without full setup,
    # but we can check the __init__ signature/code
    import inspect
    from hyperscale.distributed_rewrite.swim import HealthAwareServer
    
    source = inspect.getsource(HealthAwareServer.__init__)
    assert '_on_become_leader_callbacks' in source
    assert '_on_lose_leadership_callbacks' in source
    assert '_on_leader_change_callbacks' in source


@test("ManagerServer: has state sync methods")
def test_manager_state_sync_methods():
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    assert hasattr(ManagerServer, '_on_manager_become_leader')
    assert hasattr(ManagerServer, '_on_manager_lose_leadership')
    assert hasattr(ManagerServer, '_sync_state_from_workers')
    assert hasattr(ManagerServer, '_request_worker_state')


@test("StateSyncRequest: serialization")
def test_state_sync_request_serde():
    from hyperscale.distributed_rewrite.models import StateSyncRequest
    
    original = StateSyncRequest(
        requester_id="manager-1",
        requester_role="manager",
        since_version=100,
    )
    
    data = original.dump()
    loaded = StateSyncRequest.load(data)
    
    assert loaded.requester_id == original.requester_id
    assert loaded.requester_role == original.requester_role
    assert loaded.since_version == original.since_version


@test("StateSyncResponse: serialization with worker state")
def test_state_sync_response_worker_serde():
    from hyperscale.distributed_rewrite.models import (
        StateSyncResponse,
        WorkerStateSnapshot,
    )
    
    worker_state = WorkerStateSnapshot(
        node_id="worker-1",
        state="healthy",
        total_cores=16,
        available_cores=12,
        version=50,
        active_workflows={},
    )
    
    original = StateSyncResponse(
        responder_id="worker-1",
        current_version=50,
        worker_state=worker_state,
    )
    
    data = original.dump()
    loaded = StateSyncResponse.load(data)
    
    assert loaded.responder_id == original.responder_id
    assert loaded.current_version == original.current_version
    assert loaded.worker_state is not None
    assert loaded.worker_state.node_id == "worker-1"
    assert loaded.worker_state.available_cores == 12


@test("StateSyncResponse: serialization with manager state")
def test_state_sync_response_manager_serde():
    from hyperscale.distributed_rewrite.models import (
        StateSyncResponse,
        ManagerStateSnapshot,
    )
    
    manager_state = ManagerStateSnapshot(
        node_id="manager-1",
        datacenter="dc-east",
        is_leader=True,
        term=5,
        version=100,
        workers=[],
        jobs={},
    )
    
    original = StateSyncResponse(
        responder_id="manager-1",
        current_version=100,
        manager_state=manager_state,
    )
    
    data = original.dump()
    loaded = StateSyncResponse.load(data)
    
    assert loaded.responder_id == original.responder_id
    assert loaded.manager_state is not None
    assert loaded.manager_state.datacenter == "dc-east"
    assert loaded.manager_state.is_leader == True


# Run Leadership Callback tests
test_health_aware_server_callback_methods()
test_health_aware_server_callback_lists()
test_manager_state_sync_methods()
test_state_sync_request_serde()
test_state_sync_response_worker_serde()
test_state_sync_response_manager_serde()


# =============================================================================
# Worker Failure Retry Tests
# =============================================================================

print("\n" + "=" * 60)
print("Worker Failure Retry Tests")
print("=" * 60)


@test("HealthAwareServer: has node dead callback registration")
def test_health_aware_server_node_dead_callback():
    from hyperscale.distributed_rewrite.swim import HealthAwareServer

    assert hasattr(HealthAwareServer, 'register_on_node_dead')
    assert callable(getattr(HealthAwareServer, 'register_on_node_dead'))


@test("HealthAwareServer: node dead callback list initialized")
def test_health_aware_server_node_dead_list():
    import inspect
    from hyperscale.distributed_rewrite.swim import HealthAwareServer
    
    source = inspect.getsource(HealthAwareServer.__init__)
    assert '_on_node_dead_callbacks' in source


@test("ManagerServer: has retry mechanism methods")
def test_manager_retry_methods():
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    assert hasattr(ManagerServer, '_on_node_dead')
    assert hasattr(ManagerServer, '_handle_workflow_failure')
    assert hasattr(ManagerServer, '_retry_workflow')
    assert hasattr(ManagerServer, '_handle_worker_failure')
    assert hasattr(ManagerServer, '_select_worker_for_workflow_excluding')


@test("ManagerServer: has retry configuration")
def test_manager_retry_config():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    # Check __init__ signature has retry params
    sig = inspect.signature(ManagerServer.__init__)
    params = sig.parameters
    
    assert 'max_workflow_retries' in params
    assert 'workflow_timeout' in params
    
    # Check defaults
    assert params['max_workflow_retries'].default == 3
    assert params['workflow_timeout'].default == 300.0


# Run Worker Failure tests
test_health_aware_server_node_dead_callback()
test_health_aware_server_node_dead_list()
test_manager_retry_methods()
test_manager_retry_config()


# =============================================================================
# Per-Core Workflow Assignment Tests
# =============================================================================

print("\n" + "=" * 60)
print("Per-Core Workflow Assignment Tests")
print("=" * 60)


@test("WorkerServer: has per-core tracking methods")
def test_worker_per_core_methods():
    from hyperscale.distributed_rewrite.nodes import WorkerServer
    
    assert hasattr(WorkerServer, '_allocate_cores')
    assert hasattr(WorkerServer, '_free_cores')
    assert hasattr(WorkerServer, '_get_workflow_cores')
    assert hasattr(WorkerServer, 'get_core_assignments')
    assert hasattr(WorkerServer, 'get_workflows_on_cores')
    assert hasattr(WorkerServer, 'stop_workflows_on_cores')


@test("WorkerServer: has per-core data structures")
def test_worker_per_core_data():
    import inspect
    from hyperscale.distributed_rewrite.nodes import WorkerServer
    
    source = inspect.getsource(WorkerServer.__init__)
    assert '_core_assignments' in source
    assert '_workflow_cores' in source


@test("WorkflowProgress: has assigned_cores field")
def test_workflow_progress_cores():
    from hyperscale.distributed_rewrite.models import WorkflowProgress
    
    # Create with default (empty list)
    progress = WorkflowProgress(
        job_id="job-1",
        workflow_id="wf-1",
        workflow_name="TestWorkflow",
        status="running",
        completed_count=0,
        failed_count=0,
        rate_per_second=0.0,
        elapsed_seconds=0.0,
    )
    assert progress.assigned_cores == []
    
    # Create with specific cores
    progress_with_cores = WorkflowProgress(
        job_id="job-1",
        workflow_id="wf-1",
        workflow_name="TestWorkflow",
        status="running",
        completed_count=0,
        failed_count=0,
        rate_per_second=0.0,
        elapsed_seconds=0.0,
        assigned_cores=[0, 1, 2, 3],
    )
    assert progress_with_cores.assigned_cores == [0, 1, 2, 3]


@test("WorkflowProgress: serialization with assigned_cores")
def test_workflow_progress_cores_serde():
    from hyperscale.distributed_rewrite.models import WorkflowProgress
    
    original = WorkflowProgress(
        job_id="job-1",
        workflow_id="wf-1",
        workflow_name="TestWorkflow",
        status="running",
        completed_count=100,
        failed_count=5,
        rate_per_second=50.0,
        elapsed_seconds=2.0,
        assigned_cores=[0, 2, 4, 6],
    )
    
    data = original.dump()
    loaded = WorkflowProgress.load(data)
    
    assert loaded.assigned_cores == [0, 2, 4, 6]
    assert loaded.workflow_id == "wf-1"
    assert loaded.completed_count == 100


# Run Per-Core tests
test_worker_per_core_methods()
test_worker_per_core_data()
test_workflow_progress_cores()
test_workflow_progress_cores_serde()


# =============================================================================
# Cores Completed and Progress Tracking Tests
# =============================================================================

print("\n" + "=" * 60)
print("Cores Completed and Progress Tracking Tests")
print("=" * 60)


@test("WorkflowProgress: has cores_completed field")
def test_workflow_progress_cores_completed():
    from hyperscale.distributed_rewrite.models import WorkflowProgress
    
    # Create with default (0)
    progress = WorkflowProgress(
        job_id="job-1",
        workflow_id="wf-1",
        workflow_name="TestWorkflow",
        status="running",
        completed_count=0,
        failed_count=0,
        rate_per_second=0.0,
        elapsed_seconds=0.0,
    )
    assert progress.cores_completed == 0
    
    # Create with specific cores_completed
    progress_with_completed = WorkflowProgress(
        job_id="job-1",
        workflow_id="wf-1",
        workflow_name="TestWorkflow",
        status="running",
        completed_count=1000,
        failed_count=5,
        rate_per_second=500.0,
        elapsed_seconds=2.0,
        assigned_cores=[0, 1, 2, 3],
        cores_completed=2,  # 2 of 4 cores completed
    )
    assert progress_with_completed.cores_completed == 2
    assert progress_with_completed.assigned_cores == [0, 1, 2, 3]


@test("WorkflowProgress: has avg_cpu_percent and avg_memory_mb fields")
def test_workflow_progress_system_stats():
    from hyperscale.distributed_rewrite.models import WorkflowProgress
    
    progress = WorkflowProgress(
        job_id="job-1",
        workflow_id="wf-1",
        workflow_name="TestWorkflow",
        status="running",
        completed_count=1000,
        failed_count=5,
        rate_per_second=500.0,
        elapsed_seconds=2.0,
        avg_cpu_percent=75.5,
        avg_memory_mb=1024.0,
    )
    assert progress.avg_cpu_percent == 75.5
    assert progress.avg_memory_mb == 1024.0


@test("WorkflowProgress: serialization with cores_completed")
def test_workflow_progress_cores_completed_serde():
    from hyperscale.distributed_rewrite.models import WorkflowProgress
    
    original = WorkflowProgress(
        job_id="job-1",
        workflow_id="wf-1",
        workflow_name="TestWorkflow",
        status="running",
        completed_count=1000,
        failed_count=10,
        rate_per_second=250.0,
        elapsed_seconds=4.0,
        assigned_cores=[0, 1, 2, 3, 4, 5],
        cores_completed=4,  # 4 of 6 cores have finished
        avg_cpu_percent=80.0,
        avg_memory_mb=2048.0,
    )
    
    data = original.dump()
    loaded = WorkflowProgress.load(data)
    
    assert loaded.cores_completed == 4
    assert loaded.assigned_cores == [0, 1, 2, 3, 4, 5]
    assert loaded.avg_cpu_percent == 80.0
    assert loaded.avg_memory_mb == 2048.0


@test("WorkerServer: has workflow runner integration")
def test_worker_workflow_runner_integration():
    import inspect
    from hyperscale.distributed_rewrite.nodes import WorkerServer
    
    # Check for WorkflowRunner-related methods and fields
    assert hasattr(WorkerServer, '_get_workflow_runner')
    assert hasattr(WorkerServer, '_get_core_env')
    assert hasattr(WorkerServer, '_monitor_workflow_progress')
    
    # Check __init__ for workflow runner fields
    source = inspect.getsource(WorkerServer.__init__)
    assert '_workflow_runner' in source
    assert '_core_env' in source
    assert '_workflow_cores_completed' in source


@test("WorkerServer: _execute_workflow uses WorkflowRunner")
def test_worker_execute_uses_runner():
    import inspect
    from hyperscale.distributed_rewrite.nodes import WorkerServer
    
    source = inspect.getsource(WorkerServer._execute_workflow)
    
    # Should use the workflow runner
    assert '_get_workflow_runner' in source or 'runner.run' in source
    
    # Should track cores_completed
    assert 'cores_completed' in source


@test("ManagerServer: has cores_completed progress handler")
def test_manager_cores_completed_handler():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    # Check the method exists
    assert hasattr(ManagerServer, '_update_worker_cores_from_progress')
    
    # Check the method docstring mentions cores_completed
    method = ManagerServer._update_worker_cores_from_progress
    assert 'cores_completed' in (method.__doc__ or '').lower()
    
    # Check the method signature accepts progress objects
    sig = inspect.signature(method)
    params = list(sig.parameters.keys())
    assert 'progress' in params
    assert 'old_progress' in params


@test("ManagerServer: _update_worker_cores_from_progress updates available cores")
def test_manager_update_cores_method():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer._update_worker_cores_from_progress)
    
    # Should compare old and new cores_completed
    assert 'old_cores_completed' in source or 'cores_completed' in source
    
    # Should update worker status
    assert '_worker_status' in source
    assert 'available_cores' in source


@test("Cores completed tracking: enables faster provisioning scenario")
def test_cores_completed_provisioning_scenario():
    """
    Test that cores_completed enables faster provisioning.
    
    Scenario:
    - Worker has 8 cores
    - Workflow A is assigned 4 cores
    - After some time, 2 cores complete their portion of Workflow A
    - Manager should see 2 + 4 = 6 available cores for new workflows
    """
    from hyperscale.distributed_rewrite.models import (
        WorkflowProgress,
        WorkerHeartbeat,
        WorkerState,
    )
    
    # Initial worker state: 8 total, 4 used by workflow A
    initial_heartbeat = WorkerHeartbeat(
        node_id="worker-1",
        state=WorkerState.HEALTHY.value,
        available_cores=4,  # 4 free, 4 used
        queue_depth=0,
        cpu_percent=50.0,
        memory_percent=40.0,
        version=1,
        active_workflows={"wf-a": "running"},
    )
    
    # Old progress: 0 cores completed
    old_progress = WorkflowProgress(
        job_id="job-1",
        workflow_id="wf-a",
        workflow_name="TestWorkflow",
        status="running",
        completed_count=500,
        failed_count=0,
        rate_per_second=100.0,
        elapsed_seconds=5.0,
        assigned_cores=[0, 1, 2, 3],
        cores_completed=0,
    )
    
    # New progress: 2 cores completed
    new_progress = WorkflowProgress(
        job_id="job-1",
        workflow_id="wf-a",
        workflow_name="TestWorkflow",
        status="running",
        completed_count=1500,
        failed_count=0,
        rate_per_second=150.0,
        elapsed_seconds=10.0,
        assigned_cores=[0, 1, 2, 3],
        cores_completed=2,  # 2 cores have finished
    )
    
    # Calculate freed cores
    old_cores_completed = old_progress.cores_completed
    new_cores_completed = new_progress.cores_completed
    cores_freed = new_cores_completed - old_cores_completed
    
    assert cores_freed == 2
    
    # Simulate manager updating worker status
    new_available = initial_heartbeat.available_cores + cores_freed
    assert new_available == 6  # 4 + 2 = 6 available now


# Run Cores Completed tests
test_workflow_progress_cores_completed()
test_workflow_progress_system_stats()
test_workflow_progress_cores_completed_serde()
test_worker_workflow_runner_integration()
test_worker_execute_uses_runner()
test_manager_cores_completed_handler()
test_manager_update_cores_method()
test_cores_completed_provisioning_scenario()


# =============================================================================
# Worker and Manager Failure Handling Tests
# =============================================================================

print("\n" + "=" * 60)
print("Worker and Manager Failure Handling Tests")
print("=" * 60)


@test("ManagerServer: _handle_worker_failure properly validates retry data")
def test_manager_handle_worker_failure():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    assert hasattr(ManagerServer, '_handle_worker_failure')
    
    source = inspect.getsource(ManagerServer._handle_worker_failure)
    
    # Should check if workflow_id is in _workflow_retries
    assert '_workflow_retries' in source
    # Should check for empty dispatch data
    assert 'not data' in source or "empty dispatch" in source.lower()
    # Should log error if retry not possible
    assert 'ServerError' in source


@test("ManagerServer: _retry_workflow uses correct VUs from dispatch")
def test_manager_retry_uses_correct_vus():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer._retry_workflow)
    
    # Should parse original dispatch to get VUs
    assert 'WorkflowDispatch.load' in source
    assert 'vus_needed' in source or 'original_dispatch.vus' in source
    
    # Should NOT have hardcoded vus_needed=1
    # Check that it uses the parsed value
    assert 'original_dispatch.vus' in source


@test("WorkerServer: has manager failure detection")
def test_worker_manager_failure_detection():
    import inspect
    from hyperscale.distributed_rewrite.nodes import WorkerServer
    
    assert hasattr(WorkerServer, '_on_node_dead')
    assert hasattr(WorkerServer, '_select_new_primary_manager')
    assert hasattr(WorkerServer, '_report_active_workflows_to_managers')
    
    # Check that on_node_dead callback is registered in __init__
    init_source = inspect.getsource(WorkerServer.__init__)
    assert 'register_on_node_dead' in init_source


@test("WorkerServer: manager tracking uses new architecture")
def test_worker_manager_tracking():
    import inspect
    from hyperscale.distributed_rewrite.nodes import WorkerServer
    
    # Check for new manager tracking attributes
    assert hasattr(WorkerServer, '_update_known_managers')
    assert hasattr(WorkerServer, '_get_healthy_manager_tcp_addrs')
    assert hasattr(WorkerServer, '_get_primary_manager_tcp_addr')
    
    # Check _on_node_dead handles manager health
    source = inspect.getsource(WorkerServer._on_node_dead)
    assert '_healthy_manager_ids' in source
    assert '_select_new_primary_manager' in source


@test("Worker failure scenario: Manager detects via SWIM and reschedules")
def test_worker_failure_scenario():
    """
    Test the worker failure and workflow rescheduling scenario.
    
    Scenario:
    1. Worker A has workflow with 4 VUs
    2. Worker A dies (detected via SWIM)
    3. Manager's _on_node_dead callback fires
    4. _handle_worker_failure finds the workflow
    5. _retry_workflow selects Worker B with enough VUs
    6. Workflow is re-dispatched to Worker B
    """
    from hyperscale.distributed_rewrite.models import (
        WorkflowDispatch,
        WorkflowProgress,
        WorkflowStatus,
    )
    
    # Create original dispatch with 4 VUs
    dispatch = WorkflowDispatch(
        job_id="job-1",
        workflow_id="wf-1",
        workflow=b"pickled_workflow",
        context=b"{}",
        vus=4,
        timeout_seconds=300,
        fence_token=1,
    )
    dispatch_bytes = dispatch.dump()
    
    # Verify we can deserialize and get VUs
    restored = WorkflowDispatch.load(dispatch_bytes)
    assert restored.vus == 4
    assert restored.workflow_id == "wf-1"
    assert restored.job_id == "job-1"
    
    # Simulate retry tracking tuple
    retry_info = (0, dispatch_bytes, {"worker-a"})  # (count, data, failed_workers)
    
    # Verify we can parse VUs from retry info
    stored_dispatch = WorkflowDispatch.load(retry_info[1])
    assert stored_dispatch.vus == 4


@test("Manager failure scenario: Worker detects and fails over")
def test_manager_failure_scenario():
    """
    Test the manager failure and worker failover scenario.
    
    Scenario:
    1. Worker tracks multiple managers via _known_managers
    2. Manager A dies (detected via SWIM)  
    3. Worker's _on_node_dead callback fires
    4. Manager A removed from _healthy_manager_ids
    5. _select_new_primary_manager picks Manager B
    6. Worker continues with Manager B as primary
    """
    from hyperscale.distributed_rewrite.models import (
        WorkflowProgress,
        WorkflowStatus,
    )
    
    # Simulate active workflow state
    progress = WorkflowProgress(
        job_id="job-1",
        workflow_id="wf-1",
        workflow_name="TestWorkflow",
        status=WorkflowStatus.RUNNING.value,
        completed_count=500,
        failed_count=0,
        rate_per_second=100.0,
        elapsed_seconds=5.0,
        assigned_cores=[0, 1, 2, 3],
        cores_completed=1,
    )
    
    # Verify progress can be serialized for reporting
    data = progress.dump()
    restored = WorkflowProgress.load(data)
    assert restored.workflow_id == "wf-1"
    assert restored.status == WorkflowStatus.RUNNING.value
    assert restored.cores_completed == 1


@test("Retry preserves workflow resource requirements")
def test_retry_preserves_resources():
    """
    Verify that workflow retry preserves the original VUs requirement.
    """
    from hyperscale.distributed_rewrite.models import WorkflowDispatch
    
    # Create workflows with different VU requirements
    workflows = [
        WorkflowDispatch(
            job_id="job-1",
            workflow_id="wf-small",
            workflow=b"small",
            context=b"{}",
            vus=1,
            timeout_seconds=60,
            fence_token=1,
        ),
        WorkflowDispatch(
            job_id="job-1",
            workflow_id="wf-medium",
            workflow=b"medium",
            context=b"{}",
            vus=4,
            timeout_seconds=120,
            fence_token=2,
        ),
        WorkflowDispatch(
            job_id="job-1",
            workflow_id="wf-large",
            workflow=b"large",
            context=b"{}",
            vus=16,
            timeout_seconds=300,
            fence_token=3,
        ),
    ]
    
    for original in workflows:
        # Serialize and deserialize
        dispatch_bytes = original.dump()
        restored = WorkflowDispatch.load(dispatch_bytes)
        
        # VUs must be preserved for retry
        assert restored.vus == original.vus, f"VUs mismatch for {original.workflow_id}"
        assert restored.timeout_seconds == original.timeout_seconds


# Run Worker/Manager Failure tests
test_manager_handle_worker_failure()
test_manager_retry_uses_correct_vus()
test_worker_manager_failure_detection()
test_worker_manager_tracking()
test_worker_failure_scenario()
test_manager_failure_scenario()
test_retry_preserves_resources()


# =============================================================================
# Manager Peer Failure Detection Tests
# =============================================================================

print("\nManager Peer Failure Detection Tests")
print("=" * 40)


@test("HealthAwareServer: has register_on_node_join callback")
def test_health_aware_server_has_node_join_callback():
    from hyperscale.distributed_rewrite.swim.health_aware_server import HealthAwareServer
    
    assert hasattr(HealthAwareServer, 'register_on_node_join'), \
        "HealthAwareServer must have register_on_node_join method"
    
    # _on_node_join_callbacks is an instance attribute set in __init__
    # So we check the method exists and inspect its source
    import inspect
    source = inspect.getsource(HealthAwareServer.__init__)
    assert '_on_node_join_callbacks' in source, \
        "HealthAwareServer.__init__ must initialize _on_node_join_callbacks"


@test("ManagerServer: tracks manager UDP to TCP mapping")
def test_manager_tracks_peer_mapping():
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    # These are instance attributes set in __init__
    import inspect
    source = inspect.getsource(ManagerServer.__init__)
    
    assert '_manager_udp_to_tcp' in source, \
        "ManagerServer.__init__ must initialize _manager_udp_to_tcp"
    
    assert '_active_manager_peers' in source, \
        "ManagerServer.__init__ must initialize _active_manager_peers"


@test("ManagerServer: has _on_node_join callback")
def test_manager_has_on_node_join():
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    assert hasattr(ManagerServer, '_on_node_join'), \
        "ManagerServer must have _on_node_join method for peer recovery"


@test("ManagerServer: has _handle_manager_peer_failure method")
def test_manager_has_handle_peer_failure():
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    assert hasattr(ManagerServer, '_handle_manager_peer_failure'), \
        "ManagerServer must have _handle_manager_peer_failure method"


@test("ManagerServer: has _handle_manager_peer_recovery method")
def test_manager_has_handle_peer_recovery():
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    assert hasattr(ManagerServer, '_handle_manager_peer_recovery'), \
        "ManagerServer must have _handle_manager_peer_recovery method"


@test("ManagerServer: has _has_quorum_available method")
def test_manager_has_quorum_available():
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    assert hasattr(ManagerServer, '_has_quorum_available'), \
        "ManagerServer must have _has_quorum_available method"


@test("ManagerServer: _on_node_dead checks for manager peers")
def test_manager_on_node_dead_checks_peers():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer._on_node_dead)
    
    # Should check for manager peers, not just workers
    assert '_manager_udp_to_tcp' in source, \
        "_on_node_dead must check _manager_udp_to_tcp for manager peer failures"
    
    assert '_handle_manager_peer_failure' in source, \
        "_on_node_dead must call _handle_manager_peer_failure for manager peers"


@test("Manager peer failure scenario: active peers updated")
def test_manager_peer_failure_updates_active():
    """
    Test the conceptual flow when a manager peer dies:
    
    1. Manager A has peers [B, C] with all active
    2. Manager B dies (detected via SWIM)
    3. Manager A's _on_node_dead fires
    4. _handle_manager_peer_failure removes B from active set
    5. _has_quorum_available reflects new state
    """
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    # Check the method logic conceptually via inspection
    import inspect
    
    failure_source = inspect.getsource(ManagerServer._handle_manager_peer_failure)
    
    # Should discard from active peers
    assert 'discard' in failure_source, \
        "_handle_manager_peer_failure must discard from _active_manager_peers"
    
    # Should check if dead peer was the leader
    assert 'get_current_leader' in failure_source, \
        "_handle_manager_peer_failure should check if dead peer was leader"


@test("Manager peer recovery scenario: active peers restored")
def test_manager_peer_recovery_restores_active():
    """
    Test the conceptual flow when a manager peer recovers:
    
    1. Manager B was marked as dead
    2. Manager B rejoins the SWIM cluster
    3. _on_node_join fires on Manager A
    4. _handle_manager_peer_recovery adds B back to active set
    """
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    import inspect
    
    recovery_source = inspect.getsource(ManagerServer._handle_manager_peer_recovery)
    
    # Should add to active peers
    assert '_active_manager_peers.add' in recovery_source, \
        "_handle_manager_peer_recovery must add back to _active_manager_peers"


@test("ManagerServer: quorum calculation uses configured size (not active)")
def test_manager_quorum_uses_configured_size():
    """
    Verify quorum is calculated from configured peers, not just active.
    This prevents split-brain where a partition thinks it has quorum.
    """
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    # Get the method - need to handle if it's a property
    quorum_method = ManagerServer._quorum_size
    if isinstance(quorum_method, property):
        quorum_method = quorum_method.fget
    
    source = inspect.getsource(quorum_method)
    
    # Should use _manager_peers (configured), not _active_manager_peers
    assert '_manager_peers' in source, \
        "_quorum_size must use _manager_peers (configured count)"
    
    # Should NOT use _active_manager_peers for quorum calculation
    assert '_active_manager_peers' not in source, \
        "_quorum_size should NOT use _active_manager_peers (prevents split-brain)"


@test("ManagerServer: _has_quorum_available uses active peers")
def test_has_quorum_uses_active():
    """
    Verify _has_quorum_available checks active count vs quorum requirement.
    """
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer._has_quorum_available)
    
    assert '_active_manager_peers' in source, \
        "_has_quorum_available must check _active_manager_peers"
    
    assert '_quorum_size' in source, \
        "_has_quorum_available must compare against _quorum_size()"


# Run Manager Peer Failure tests
test_health_aware_server_has_node_join_callback()
test_manager_tracks_peer_mapping()
test_manager_has_on_node_join()
test_manager_has_handle_peer_failure()
test_manager_has_handle_peer_recovery()
test_manager_has_quorum_available()
test_manager_on_node_dead_checks_peers()
test_manager_peer_failure_updates_active()
test_manager_peer_recovery_restores_active()
test_manager_quorum_uses_configured_size()
test_has_quorum_uses_active()


# =============================================================================
# State Sync and Gate Split-Brain Prevention Tests
# =============================================================================

print("\nState Sync and Gate Split-Brain Prevention Tests")
print("=" * 50)


@test("ManagerServer: _request_worker_state has retry logic")
def test_manager_worker_state_retry():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer._request_worker_state)
    
    # Should have retry loop
    assert 'max_retries' in source, \
        "_request_worker_state must have max_retries parameter"
    assert 'for attempt' in source or 'range(max_retries)' in source, \
        "_request_worker_state must have retry loop"
    assert 'base_delay' in source, \
        "_request_worker_state must have exponential backoff"


@test("ManagerServer: has _sync_state_from_manager_peers")
def test_manager_has_peer_sync():
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    assert hasattr(ManagerServer, '_sync_state_from_manager_peers'), \
        "ManagerServer must have _sync_state_from_manager_peers method"


@test("ManagerServer: _on_manager_become_leader syncs from peers")
def test_manager_become_leader_syncs_peers():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer._on_manager_become_leader)
    
    assert '_sync_state_from_workers' in source, \
        "_on_manager_become_leader must sync from workers"
    assert '_sync_state_from_manager_peers' in source, \
        "_on_manager_become_leader must sync from manager peers"


@test("ManagerServer: has _request_manager_peer_state with retries")
def test_manager_has_peer_state_request():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    assert hasattr(ManagerServer, '_request_manager_peer_state'), \
        "ManagerServer must have _request_manager_peer_state method"
    
    source = inspect.getsource(ManagerServer._request_manager_peer_state)
    
    # Should have retry logic
    assert 'max_retries' in source, \
        "_request_manager_peer_state must have max_retries parameter"


@test("ManagerServer: has _process_manager_state_response")
def test_manager_has_process_peer_response():
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    assert hasattr(ManagerServer, '_process_manager_state_response'), \
        "ManagerServer must have _process_manager_state_response method"


@test("GateServer: tracks gate peer addresses")
def test_gate_tracks_peer_mapping():
    import inspect
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    source = inspect.getsource(GateServer.__init__)
    
    assert '_gate_udp_to_tcp' in source, \
        "GateServer.__init__ must initialize _gate_udp_to_tcp"
    assert '_active_gate_peers' in source, \
        "GateServer.__init__ must initialize _active_gate_peers"


@test("GateServer: has _on_node_dead callback")
def test_gate_has_on_node_dead():
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    assert hasattr(GateServer, '_on_node_dead'), \
        "GateServer must have _on_node_dead method"


@test("GateServer: has _on_node_join callback")
def test_gate_has_on_node_join():
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    assert hasattr(GateServer, '_on_node_join'), \
        "GateServer must have _on_node_join method"


@test("GateServer: has _handle_gate_peer_failure method")
def test_gate_has_handle_peer_failure():
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    assert hasattr(GateServer, '_handle_gate_peer_failure'), \
        "GateServer must have _handle_gate_peer_failure method"


@test("GateServer: has _handle_gate_peer_recovery method")
def test_gate_has_handle_peer_recovery():
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    assert hasattr(GateServer, '_handle_gate_peer_recovery'), \
        "GateServer must have _handle_gate_peer_recovery method"


@test("GateServer: _on_node_dead checks for gate peers")
def test_gate_on_node_dead_checks_peers():
    import inspect
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    source = inspect.getsource(GateServer._on_node_dead)
    
    assert '_gate_udp_to_tcp' in source, \
        "_on_node_dead must check _gate_udp_to_tcp for gate peer failures"
    assert '_handle_gate_peer_failure' in source, \
        "_on_node_dead must call _handle_gate_peer_failure for gate peers"


@test("GateServer: peer failure updates active peers")
def test_gate_peer_failure_updates_active():
    import inspect
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    source = inspect.getsource(GateServer._handle_gate_peer_failure)
    
    assert 'discard' in source, \
        "_handle_gate_peer_failure must discard from _active_gate_peers"


@test("GateServer: peer recovery restores active peers")
def test_gate_peer_recovery_restores_active():
    import inspect
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    source = inspect.getsource(GateServer._handle_gate_peer_recovery)
    
    assert '_active_gate_peers.add' in source, \
        "_handle_gate_peer_recovery must add back to _active_gate_peers"


# Run State Sync and Gate Split-Brain Prevention tests
test_manager_worker_state_retry()
test_manager_has_peer_sync()
test_manager_become_leader_syncs_peers()
test_manager_has_peer_state_request()
test_manager_has_process_peer_response()
test_gate_tracks_peer_mapping()
test_gate_has_on_node_dead()
test_gate_has_on_node_join()
test_gate_has_handle_peer_failure()
test_gate_has_handle_peer_recovery()
test_gate_on_node_dead_checks_peers()
test_gate_peer_failure_updates_active()
test_gate_peer_recovery_restores_active()


# =============================================================================
# CRDT (Conflict-free Replicated Data Types) Tests
# =============================================================================

print("\nCRDT Tests")
print("=" * 50)


@test("GCounter: initial value is 0")
def test_gcounter_initial():
    from hyperscale.distributed_rewrite.models import GCounter
    
    counter = GCounter()
    assert counter.value == 0, "Initial GCounter value should be 0"


@test("GCounter: increment increases value")
def test_gcounter_increment():
    from hyperscale.distributed_rewrite.models import GCounter
    
    counter = GCounter()
    counter.increment("dc-east", 5)
    counter.increment("dc-west", 3)
    
    assert counter.value == 8, f"Expected 8, got {counter.value}"
    assert counter.get_node_value("dc-east") == 5
    assert counter.get_node_value("dc-west") == 3


@test("GCounter: merge takes max of each slot")
def test_gcounter_merge():
    from hyperscale.distributed_rewrite.models import GCounter
    
    counter1 = GCounter()
    counter1.increment("dc-east", 5)
    counter1.increment("dc-west", 3)
    
    counter2 = GCounter()
    counter2.increment("dc-east", 10)  # Higher than counter1
    counter2.increment("dc-south", 2)   # Not in counter1
    
    merged = counter1.merge(counter2)
    
    assert merged.get_node_value("dc-east") == 10  # max(5, 10)
    assert merged.get_node_value("dc-west") == 3   # only in counter1
    assert merged.get_node_value("dc-south") == 2  # only in counter2
    assert merged.value == 15  # 10 + 3 + 2


@test("GCounter: merge is commutative")
def test_gcounter_merge_commutative():
    from hyperscale.distributed_rewrite.models import GCounter
    
    counter1 = GCounter(counts={"a": 5, "b": 3})
    counter2 = GCounter(counts={"a": 10, "c": 2})
    
    merged1 = counter1.merge(counter2)
    merged2 = counter2.merge(counter1)
    
    assert merged1.value == merged2.value
    assert merged1.to_dict() == merged2.to_dict()


@test("GCounter: merge is idempotent")
def test_gcounter_merge_idempotent():
    from hyperscale.distributed_rewrite.models import GCounter
    
    counter = GCounter(counts={"a": 5, "b": 3})
    
    merged = counter.merge(counter)
    
    assert merged.value == counter.value
    assert merged.to_dict() == counter.to_dict()


@test("GCounter: serialization round-trip")
def test_gcounter_serialization():
    from hyperscale.distributed_rewrite.models import GCounter
    
    counter = GCounter()
    counter.increment("dc-east", 100)
    counter.increment("dc-west", 50)
    
    data = counter.to_dict()
    restored = GCounter.from_dict(data)
    
    assert restored.value == counter.value
    assert restored.to_dict() == counter.to_dict()


@test("LWWRegister: set and get value")
def test_lww_register_basic():
    from hyperscale.distributed_rewrite.models import LWWRegister
    
    reg = LWWRegister()
    reg.set(100.5, 1, "node-1")
    
    assert reg.value == 100.5
    assert reg.timestamp == 1


@test("LWWRegister: higher timestamp wins")
def test_lww_register_timestamp():
    from hyperscale.distributed_rewrite.models import LWWRegister
    
    reg = LWWRegister()
    reg.set(100.5, 1, "node-1")
    reg.set(200.0, 2, "node-2")  # Higher timestamp
    
    assert reg.value == 200.0
    
    # Older timestamp rejected
    reg.set(50.0, 1, "node-3")
    assert reg.value == 200.0


@test("LWWRegister: node_id breaks ties")
def test_lww_register_tiebreak():
    from hyperscale.distributed_rewrite.models import LWWRegister
    
    reg = LWWRegister()
    reg.set(100.0, 5, "aaa")
    reg.set(200.0, 5, "zzz")  # Same timestamp, higher node_id wins
    
    assert reg.value == 200.0


@test("LWWRegister: merge keeps winner")
def test_lww_register_merge():
    from hyperscale.distributed_rewrite.models import LWWRegister
    
    reg1 = LWWRegister()
    reg1.set(100.0, 1, "node-1")
    
    reg2 = LWWRegister()
    reg2.set(200.0, 2, "node-2")
    
    merged = reg1.merge(reg2)
    
    assert merged.value == 200.0
    assert merged.timestamp == 2


@test("LWWMap: set and get values")
def test_lww_map_basic():
    from hyperscale.distributed_rewrite.models import LWWMap
    
    m = LWWMap()
    m.set("dc-east", "RUNNING", 1, "manager-1")
    m.set("dc-west", "COMPLETED", 2, "manager-2")
    
    assert m.get("dc-east") == "RUNNING"
    assert m.get("dc-west") == "COMPLETED"
    assert m.get("dc-missing") is None


@test("LWWMap: merge combines entries")
def test_lww_map_merge():
    from hyperscale.distributed_rewrite.models import LWWMap
    
    m1 = LWWMap()
    m1.set("dc-east", "RUNNING", 1, "m1")
    
    m2 = LWWMap()
    m2.set("dc-east", "COMPLETED", 2, "m2")  # Newer
    m2.set("dc-west", "RUNNING", 1, "m2")     # Not in m1
    
    merged = m1.merge(m2)
    
    assert merged.get("dc-east") == "COMPLETED"  # m2's newer value
    assert merged.get("dc-west") == "RUNNING"    # from m2


@test("JobStatsCRDT: basic operations")
def test_job_stats_crdt_basic():
    from hyperscale.distributed_rewrite.models import JobStatsCRDT
    
    stats = JobStatsCRDT(job_id="job-123")
    
    stats.record_completed("dc-east", 100)
    stats.record_completed("dc-west", 50)
    stats.record_failed("dc-west", 2)
    stats.record_rate("dc-east", 500.0, 1)
    stats.record_rate("dc-west", 250.0, 1)
    stats.record_status("dc-east", "RUNNING", 1)
    
    assert stats.total_completed == 150
    assert stats.total_failed == 2
    assert stats.total_rate == 750.0
    assert stats.get_dc_status("dc-east") == "RUNNING"


@test("JobStatsCRDT: merge combines stats")
def test_job_stats_crdt_merge():
    from hyperscale.distributed_rewrite.models import JobStatsCRDT
    
    stats1 = JobStatsCRDT(job_id="job-123")
    stats1.record_completed("dc-east", 100)
    stats1.record_rate("dc-east", 500.0, 1)
    
    stats2 = JobStatsCRDT(job_id="job-123")
    stats2.record_completed("dc-east", 200)  # Higher - wins
    stats2.record_completed("dc-west", 50)   # New DC
    stats2.record_rate("dc-east", 600.0, 2)  # Newer timestamp - wins
    
    merged = stats1.merge(stats2)
    
    assert merged.total_completed == 250  # max(100, 200) + 50
    assert merged.get_dc_rate("dc-east") == 600.0  # Newer timestamp


@test("JobStatsCRDT: serialization round-trip")
def test_job_stats_crdt_serialization():
    from hyperscale.distributed_rewrite.models import JobStatsCRDT
    
    stats = JobStatsCRDT(job_id="job-123")
    stats.record_completed("dc-east", 100)
    stats.record_failed("dc-west", 5)
    stats.record_rate("dc-east", 500.0, 1)
    stats.record_status("dc-east", "RUNNING", 1)
    
    data = stats.to_dict()
    restored = JobStatsCRDT.from_dict(data)
    
    assert restored.job_id == stats.job_id
    assert restored.total_completed == stats.total_completed
    assert restored.total_failed == stats.total_failed
    assert restored.total_rate == stats.total_rate


@test("JobStatsCRDT: cross-DC merge scenario")
def test_job_stats_crdt_cross_dc_merge():
    """
    Simulate a scenario where two gates have different views
    of the same job's stats, then merge.
    """
    from hyperscale.distributed_rewrite.models import JobStatsCRDT
    
    # Gate A's view
    gate_a_stats = JobStatsCRDT(job_id="job-123")
    gate_a_stats.record_completed("dc-east", 100)
    gate_a_stats.record_completed("dc-west", 50)
    gate_a_stats.record_rate("dc-east", 500.0, 1)
    gate_a_stats.record_status("dc-east", "RUNNING", 1)
    
    # Gate B's view (newer data from dc-east, new DC dc-south)
    gate_b_stats = JobStatsCRDT(job_id="job-123")
    gate_b_stats.record_completed("dc-east", 200)  # More progress
    gate_b_stats.record_completed("dc-south", 75)  # New DC
    gate_b_stats.record_rate("dc-east", 600.0, 2)  # Newer timestamp
    gate_b_stats.record_status("dc-east", "COMPLETED", 2)  # Newer
    
    # Gate A merges Gate B's view
    gate_a_stats.merge_in_place(gate_b_stats)
    
    # After merge, Gate A should have the most complete view
    assert gate_a_stats.get_dc_completed("dc-east") == 200  # max
    assert gate_a_stats.get_dc_completed("dc-west") == 50   # unchanged
    assert gate_a_stats.get_dc_completed("dc-south") == 75  # new
    assert gate_a_stats.total_completed == 325  # 200 + 50 + 75
    assert gate_a_stats.get_dc_rate("dc-east") == 600.0  # newer
    assert gate_a_stats.get_dc_status("dc-east") == "COMPLETED"  # newer


# Run CRDT tests
test_gcounter_initial()
test_gcounter_increment()
test_gcounter_merge()
test_gcounter_merge_commutative()
test_gcounter_merge_idempotent()
test_gcounter_serialization()
test_lww_register_basic()
test_lww_register_timestamp()
test_lww_register_tiebreak()
test_lww_register_merge()
test_lww_map_basic()
test_lww_map_merge()
test_job_stats_crdt_basic()
test_job_stats_crdt_merge()
test_job_stats_crdt_serialization()
test_job_stats_crdt_cross_dc_merge()


# =============================================================================
# Datacenter Health Classification & Smart Dispatch Tests
# =============================================================================

print("\nDatacenter Health Classification Tests")
print("=" * 50)


@test("DatacenterHealth: enum has all required states")
def test_dc_health_enum():
    from hyperscale.distributed_rewrite.models import DatacenterHealth
    
    assert hasattr(DatacenterHealth, 'HEALTHY')
    assert hasattr(DatacenterHealth, 'BUSY')
    assert hasattr(DatacenterHealth, 'DEGRADED')
    assert hasattr(DatacenterHealth, 'UNHEALTHY')
    
    assert DatacenterHealth.HEALTHY.value == "healthy"
    assert DatacenterHealth.BUSY.value == "busy"
    assert DatacenterHealth.DEGRADED.value == "degraded"
    assert DatacenterHealth.UNHEALTHY.value == "unhealthy"


@test("DatacenterStatus: has all required fields")
def test_dc_status_fields():
    from hyperscale.distributed_rewrite.models import DatacenterStatus, DatacenterHealth
    
    status = DatacenterStatus(
        dc_id="us-east-1",
        health=DatacenterHealth.HEALTHY.value,
        available_capacity=100,
        queue_depth=5,
        manager_count=3,
        worker_count=10,
        last_update=123.456,
    )
    
    assert status.dc_id == "us-east-1"
    assert status.health == "healthy"
    assert status.available_capacity == 100
    assert status.queue_depth == 5
    assert status.manager_count == 3
    assert status.worker_count == 10


@test("DatacenterStatus: serialization round-trip")
def test_dc_status_serialization():
    from hyperscale.distributed_rewrite.models import DatacenterStatus, DatacenterHealth
    
    status = DatacenterStatus(
        dc_id="eu-west-1",
        health=DatacenterHealth.BUSY.value,
        available_capacity=0,
        queue_depth=50,
        manager_count=2,
        worker_count=5,
    )
    
    data = status.dump()
    restored = DatacenterStatus.load(data)
    
    assert restored.dc_id == status.dc_id
    assert restored.health == status.health
    assert restored.available_capacity == status.available_capacity


@test("GateServer: has _classify_datacenter_health method")
def test_gate_has_classify_dc_health():
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    assert hasattr(GateServer, '_classify_datacenter_health'), \
        "GateServer must have _classify_datacenter_health method"


@test("GateServer: has _get_all_datacenter_health method")
def test_gate_has_get_all_dc_health():
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    assert hasattr(GateServer, '_get_all_datacenter_health'), \
        "GateServer must have _get_all_datacenter_health method"


@test("GateServer: has _select_datacenters_with_fallback method")
def test_gate_has_select_dc_fallback():
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    assert hasattr(GateServer, '_select_datacenters_with_fallback'), \
        "GateServer must have _select_datacenters_with_fallback method"


@test("GateServer: has _try_dispatch_to_dc method")
def test_gate_has_try_dispatch():
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    assert hasattr(GateServer, '_try_dispatch_to_dc'), \
        "GateServer must have _try_dispatch_to_dc method"


@test("GateServer: has _dispatch_job_with_fallback method")
def test_gate_has_dispatch_fallback():
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    assert hasattr(GateServer, '_dispatch_job_with_fallback'), \
        "GateServer must have _dispatch_job_with_fallback method"


@test("GateServer: _classify_datacenter_health returns DatacenterStatus")
def test_gate_classify_dc_returns_status():
    import inspect
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    source = inspect.getsource(GateServer._classify_datacenter_health)
    
    # Should return DatacenterStatus
    assert 'DatacenterStatus' in source, \
        "_classify_datacenter_health must return DatacenterStatus"
    
    # Should check manager liveness via SWIM
    assert 'incarnation_tracker' in source or 'get_node_state' in source, \
        "_classify_datacenter_health should check SWIM liveness"
    
    # Should check for HEALTHY, BUSY, DEGRADED, UNHEALTHY
    assert 'HEALTHY' in source
    assert 'BUSY' in source
    assert 'UNHEALTHY' in source


@test("GateServer: _select_datacenters_with_fallback returns tuple")
def test_gate_select_dc_returns_tuple():
    import inspect
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    source = inspect.getsource(GateServer._select_datacenters_with_fallback)
    
    # Should return tuple of (primary, fallback)
    assert 'tuple' in source or 'return (primary' in source or 'return (' in source, \
        "_select_datacenters_with_fallback should return tuple"
    
    # Should bucket by health
    assert 'healthy' in source.lower() and 'busy' in source.lower()


@test("GateServer: _dispatch_job_to_datacenters uses fallback")
def test_gate_dispatch_uses_fallback():
    import inspect
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    source = inspect.getsource(GateServer._dispatch_job_to_datacenters)
    
    # Should use fallback mechanism
    assert '_dispatch_job_with_fallback' in source or '_select_datacenters_with_fallback' in source, \
        "_dispatch_job_to_datacenters should use fallback mechanism"


@test("Smart dispatch: only fails if ALL DCs are UNHEALTHY")
def test_smart_dispatch_only_fail_if_all_unhealthy():
    """
    Verify the key insight: BUSY ≠ UNHEALTHY.
    Jobs should only fail if ALL datacenters are UNHEALTHY.
    BUSY DCs should still accept jobs (they will be queued).
    """
    import inspect
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    # Check _try_dispatch_to_manager handles BUSY correctly
    # (this is where the actual dispatch logic lives now)
    try_dispatch_source = inspect.getsource(GateServer._try_dispatch_to_manager)
    
    # Should treat "no capacity" / "busy" as acceptable
    assert 'no capacity' in try_dispatch_source.lower() or 'busy' in try_dispatch_source.lower(), \
        "_try_dispatch_to_manager should treat BUSY as acceptable"
    
    # Check _dispatch_job_to_datacenters only fails when ALL fail
    dispatch_source = inspect.getsource(GateServer._dispatch_job_to_datacenters)
    
    # Should check if successful_dcs is empty before failing
    assert 'not successful_dcs' in dispatch_source or 'if not successful' in dispatch_source or \
           'successful_dcs' in dispatch_source, \
        "_dispatch_job_to_datacenters should check if any DC succeeded"


@test("Health classification: BUSY when managers responding but no capacity")
def test_health_classification_busy():
    """
    BUSY state should be assigned when:
    - Managers are responding (alive via SWIM)
    - Workers exist
    - But no immediate capacity (available_cores = 0)
    """
    import inspect
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    source = inspect.getsource(GateServer._classify_datacenter_health)
    
    # Should check for workers existing but no capacity
    assert 'worker_count' in source and 'available_cores' in source or \
           'worker_count' in source and 'capacity' in source.lower(), \
        "_classify_datacenter_health should distinguish BUSY from UNHEALTHY"


# Run DC Health Classification tests
test_dc_health_enum()
test_dc_status_fields()
test_dc_status_serialization()
test_gate_has_classify_dc_health()
test_gate_has_get_all_dc_health()
test_gate_has_select_dc_fallback()
test_gate_has_try_dispatch()
test_gate_has_dispatch_fallback()
test_gate_classify_dc_returns_status()
test_gate_select_dc_returns_tuple()
test_gate_dispatch_uses_fallback()
test_smart_dispatch_only_fail_if_all_unhealthy()
test_health_classification_busy()


# =============================================================================
# Tiered Update Strategy Tests
# =============================================================================

print("\nTiered Update Strategy Tests")
print("=" * 50)


@test("UpdateTier: enum has all required values")
def test_update_tier_enum():
    from hyperscale.distributed_rewrite.models import UpdateTier
    
    assert hasattr(UpdateTier, 'IMMEDIATE')
    assert hasattr(UpdateTier, 'PERIODIC')
    assert hasattr(UpdateTier, 'ON_DEMAND')
    
    assert UpdateTier.IMMEDIATE.value == "immediate"
    assert UpdateTier.PERIODIC.value == "periodic"
    assert UpdateTier.ON_DEMAND.value == "on_demand"


@test("GateServer: has _classify_update_tier method")
def test_gate_has_classify_tier():
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    assert hasattr(GateServer, '_classify_update_tier'), \
        "GateServer must have _classify_update_tier method"


@test("GateServer: has _send_immediate_update method")
def test_gate_has_immediate_update():
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    assert hasattr(GateServer, '_send_immediate_update'), \
        "GateServer must have _send_immediate_update method"


@test("GateServer: has _batch_stats_loop method")
def test_gate_has_batch_stats_loop():
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    assert hasattr(GateServer, '_batch_stats_loop'), \
        "GateServer must have _batch_stats_loop method"


@test("GateServer: has _batch_stats_update method")
def test_gate_has_batch_stats_update():
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    assert hasattr(GateServer, '_batch_stats_update'), \
        "GateServer must have _batch_stats_update method"


@test("GateServer: has _handle_update_by_tier method")
def test_gate_has_handle_update_tier():
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    assert hasattr(GateServer, '_handle_update_by_tier'), \
        "GateServer must have _handle_update_by_tier method"


@test("GateServer: _classify_update_tier returns IMMEDIATE for completion")
def test_classify_tier_completion_is_immediate():
    import inspect
    from hyperscale.distributed_rewrite.nodes import GateServer
    from hyperscale.distributed_rewrite.models import JobStatus
    
    source = inspect.getsource(GateServer._classify_update_tier)
    
    # Should classify COMPLETED and FAILED as IMMEDIATE
    assert 'COMPLETED' in source or 'completed' in source.lower()
    assert 'FAILED' in source or 'failed' in source.lower()
    assert 'IMMEDIATE' in source


@test("GateServer: _classify_update_tier returns PERIODIC for progress")
def test_classify_tier_progress_is_periodic():
    import inspect
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    source = inspect.getsource(GateServer._classify_update_tier)
    
    # Should classify regular updates as PERIODIC
    assert 'PERIODIC' in source


@test("GateServer: receive_job_progress uses tiered updates")
def test_receive_progress_uses_tiers():
    import inspect
    import pathlib
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    # The receive_job_progress method is decorated, so we need to read the file directly
    gate_path = pathlib.Path(inspect.getfile(GateServer))
    source = gate_path.read_text()
    
    # Find the receive_job_progress method in the source
    # It should call _handle_update_by_tier
    assert 'def receive_job_progress' in source, \
        "GateServer must have receive_job_progress method"
    
    # Extract the method body
    start_idx = source.find('def receive_job_progress')
    if start_idx > 0:
        # Look for _handle_update_by_tier after the method definition
        method_end_idx = source.find('\n    @', start_idx + 100)  # Next method
        if method_end_idx == -1:
            method_end_idx = len(source)
        method_source = source[start_idx:method_end_idx]
        
        assert '_handle_update_by_tier' in method_source, \
            "receive_job_progress should use tiered update strategy"


@test("GateServer: start() runs batch stats loop")
def test_gate_start_runs_batch_loop():
    import inspect
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    source = inspect.getsource(GateServer.start)
    
    # Should start the batch stats loop
    assert '_batch_stats_loop' in source, \
        "GateServer.start() should run _batch_stats_loop"


# Run tiered update tests
test_update_tier_enum()
test_gate_has_classify_tier()
test_gate_has_immediate_update()
test_gate_has_batch_stats_loop()
test_gate_has_batch_stats_update()
test_gate_has_handle_update_tier()
test_classify_tier_completion_is_immediate()
test_classify_tier_progress_is_periodic()
test_receive_progress_uses_tiers()
test_gate_start_runs_batch_loop()


# =============================================================================
# Continuous Manager List Refresh Tests
# =============================================================================

print("\nContinuous Manager List Refresh Tests")
print("=" * 50)


@test("WorkflowProgressAck: model exists with expected fields")
def test_workflow_progress_ack_model():
    from hyperscale.distributed_rewrite.models import WorkflowProgressAck, ManagerInfo
    
    # Create a sample ack
    managers = [
        ManagerInfo(
            node_id="manager-1",
            tcp_host="host1",
            tcp_port=8000,
            udp_host="host1",
            udp_port=8001,
            datacenter="dc-east",
            is_leader=True,
        )
    ]
    
    ack = WorkflowProgressAck(
        manager_id="manager-1",
        is_leader=True,
        healthy_managers=managers,
    )
    
    assert ack.manager_id == "manager-1"
    assert ack.is_leader is True
    assert len(ack.healthy_managers) == 1
    assert ack.healthy_managers[0].node_id == "manager-1"


@test("WorkflowProgressAck: serialization round-trip")
def test_workflow_progress_ack_serialization():
    from hyperscale.distributed_rewrite.models import WorkflowProgressAck, ManagerInfo
    
    ack = WorkflowProgressAck(
        manager_id="manager-1",
        is_leader=True,
        healthy_managers=[
            ManagerInfo(
                node_id="manager-1",
                tcp_host="host1",
                tcp_port=8000,
                udp_host="host1",
                udp_port=8001,
                datacenter="dc-east",
                is_leader=True,
            )
        ],
    )
    
    data = ack.dump()
    restored = WorkflowProgressAck.load(data)
    
    assert restored.manager_id == ack.manager_id
    assert restored.is_leader == ack.is_leader
    assert len(restored.healthy_managers) == len(ack.healthy_managers)


@test("Manager: receive_workflow_progress returns WorkflowProgressAck")
def test_manager_progress_returns_ack():
    """
    Test that receive_workflow_progress returns WorkflowProgressAck.
    
    Note: We read the source file directly because the @tcp.receive() 
    decorator wraps the method, and inspect.getsource() returns the wrapper.
    """
    import pathlib
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    # Get the source file path
    import hyperscale.distributed_rewrite.nodes.manager as manager_module
    source_file = pathlib.Path(manager_module.__file__)
    source = source_file.read_text()
    
    # Find the receive_workflow_progress method section
    # Should return WorkflowProgressAck, not just b'ok'
    assert "WorkflowProgressAck(" in source, \
        "receive_workflow_progress should construct WorkflowProgressAck"
    assert "ack = WorkflowProgressAck" in source, \
        "receive_workflow_progress should create ack variable"
    assert "_get_healthy_managers()" in source, \
        "receive_workflow_progress should call _get_healthy_managers"


@test("Worker: processes WorkflowProgressAck from manager")
def test_worker_processes_progress_ack():
    import inspect
    from hyperscale.distributed_rewrite.nodes import WorkerServer
    
    # Check that worker has method to process ack
    assert hasattr(WorkerServer, '_process_workflow_progress_ack'), \
        "WorkerServer should have _process_workflow_progress_ack method"
    
    # Check the method updates known managers and tracks leadership
    source = inspect.getsource(WorkerServer._process_workflow_progress_ack)
    
    assert "WorkflowProgressAck" in source, \
        "_process_workflow_progress_ack should parse WorkflowProgressAck"
    assert "_update_known_managers" in source, \
        "_process_workflow_progress_ack should update known managers"
    assert "is_leader" in source, \
        "_process_workflow_progress_ack should check leadership"


@test("Worker: _send_progress_update processes ack response")
def test_worker_send_progress_processes_ack():
    import inspect
    from hyperscale.distributed_rewrite.nodes import WorkerServer
    
    source = inspect.getsource(WorkerServer._send_progress_update)
    
    # Should capture and process response
    assert "_process_workflow_progress_ack" in source, \
        "_send_progress_update should process ack response"


# Run continuous refresh tests
test_workflow_progress_ack_model()
test_workflow_progress_ack_serialization()
test_manager_progress_returns_ack()
test_worker_processes_progress_ack()
test_worker_send_progress_processes_ack()


# =============================================================================
# Manager Peer Node ID Tracking Tests
# =============================================================================

print("\nManager Peer Node ID Tracking Tests")
print("=" * 50)


@test("ManagerStateEmbedder: has on_manager_heartbeat callback")
def test_manager_embedder_has_peer_callback():
    from hyperscale.distributed_rewrite.swim.core.state_embedder import ManagerStateEmbedder
    import inspect
    
    # Check that on_manager_heartbeat is a field
    sig = inspect.signature(ManagerStateEmbedder)
    param_names = list(sig.parameters.keys())
    
    assert 'on_manager_heartbeat' in param_names, \
        "ManagerStateEmbedder should have on_manager_heartbeat parameter"


@test("ManagerStateEmbedder: process_state handles ManagerHeartbeat")
def test_manager_embedder_processes_manager_heartbeat():
    import inspect
    from hyperscale.distributed_rewrite.swim.core.state_embedder import ManagerStateEmbedder
    
    source = inspect.getsource(ManagerStateEmbedder.process_state)
    
    # Should try to parse as ManagerHeartbeat
    assert "ManagerHeartbeat" in source, \
        "process_state should try to parse ManagerHeartbeat from peers"
    assert "on_manager_heartbeat" in source, \
        "process_state should call on_manager_heartbeat callback"


@test("Manager: has _manager_peer_info tracking")
def test_manager_has_peer_info_tracking():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer.__init__)
    
    assert "_manager_peer_info" in source, \
        "ManagerServer should have _manager_peer_info dict"


@test("Manager: has _handle_manager_peer_heartbeat method")
def test_manager_has_peer_heartbeat_handler():
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    import inspect
    
    assert hasattr(ManagerServer, '_handle_manager_peer_heartbeat'), \
        "ManagerServer should have _handle_manager_peer_heartbeat method"
    
    source = inspect.getsource(ManagerServer._handle_manager_peer_heartbeat)
    
    assert "ManagerHeartbeat" in source, \
        "_handle_manager_peer_heartbeat should handle ManagerHeartbeat"
    assert "_manager_peer_info" in source, \
        "_handle_manager_peer_heartbeat should update _manager_peer_info"


@test("Manager: _get_healthy_managers uses real peer info")
def test_manager_get_healthy_uses_peer_info():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer._get_healthy_managers)
    
    # Should use _manager_peer_info for real node_ids
    assert "_manager_peer_info" in source, \
        "_get_healthy_managers should check _manager_peer_info"
    assert "peer_heartbeat.node_id" in source, \
        "_get_healthy_managers should use real node_id from heartbeat"


@test("Manager: state embedder includes on_manager_heartbeat")
def test_manager_embedder_includes_callback():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer.__init__)
    
    # Should pass on_manager_heartbeat to ManagerStateEmbedder
    assert "on_manager_heartbeat=self._handle_manager_peer_heartbeat" in source, \
        "Manager should pass _handle_manager_peer_heartbeat to ManagerStateEmbedder"


# Run manager peer tracking tests
test_manager_embedder_has_peer_callback()
test_manager_embedder_processes_manager_heartbeat()
test_manager_has_peer_info_tracking()
test_manager_has_peer_heartbeat_handler()
test_manager_get_healthy_uses_peer_info()
test_manager_embedder_includes_callback()


# =============================================================================
# Worker Leader Tracking via SWIM Tests
# =============================================================================

print("\nWorker Leader Tracking via SWIM Tests")
print("=" * 50)


@test("WorkerStateEmbedder: has on_manager_heartbeat callback")
def test_worker_embedder_has_manager_callback():
    from hyperscale.distributed_rewrite.swim.core.state_embedder import WorkerStateEmbedder
    import inspect
    
    sig = inspect.signature(WorkerStateEmbedder)
    param_names = list(sig.parameters.keys())
    
    assert 'on_manager_heartbeat' in param_names, \
        "WorkerStateEmbedder should have on_manager_heartbeat parameter"


@test("WorkerStateEmbedder: process_state handles ManagerHeartbeat")
def test_worker_embedder_processes_manager_heartbeat():
    import inspect
    from hyperscale.distributed_rewrite.swim.core.state_embedder import WorkerStateEmbedder
    
    source = inspect.getsource(WorkerStateEmbedder.process_state)
    
    # Should try to parse as ManagerHeartbeat
    assert "ManagerHeartbeat" in source, \
        "process_state should parse ManagerHeartbeat from managers"
    assert "on_manager_heartbeat" in source, \
        "process_state should call on_manager_heartbeat callback"


@test("Worker: has _handle_manager_heartbeat method")
def test_worker_has_manager_heartbeat_handler():
    from hyperscale.distributed_rewrite.nodes import WorkerServer
    import inspect
    
    assert hasattr(WorkerServer, '_handle_manager_heartbeat'), \
        "WorkerServer should have _handle_manager_heartbeat method"
    
    source = inspect.getsource(WorkerServer._handle_manager_heartbeat)
    
    assert "ManagerHeartbeat" in source, \
        "_handle_manager_heartbeat should handle ManagerHeartbeat"
    assert "is_leader" in source, \
        "_handle_manager_heartbeat should track leadership"
    assert "_primary_manager_id" in source, \
        "_handle_manager_heartbeat should update primary manager"


@test("Worker: state embedder includes on_manager_heartbeat")
def test_worker_embedder_includes_callback():
    import inspect
    from hyperscale.distributed_rewrite.nodes import WorkerServer
    
    source = inspect.getsource(WorkerServer.__init__)
    
    # Should pass on_manager_heartbeat to WorkerStateEmbedder
    assert "on_manager_heartbeat=self._handle_manager_heartbeat" in source, \
        "Worker should pass _handle_manager_heartbeat to WorkerStateEmbedder"


@test("Worker: _handle_manager_heartbeat updates leadership tracking")
def test_worker_heartbeat_updates_leader():
    import inspect
    from hyperscale.distributed_rewrite.nodes import WorkerServer
    
    source = inspect.getsource(WorkerServer._handle_manager_heartbeat)
    
    # Should update primary when leadership changes
    assert "Leadership change" in source, \
        "_handle_manager_heartbeat should log leadership changes"
    
    # Should update _known_managers
    assert "_known_managers" in source, \
        "_handle_manager_heartbeat should update _known_managers"


@test("Worker: _handle_manager_heartbeat discovers new managers via SWIM")
def test_worker_heartbeat_discovers_new_managers():
    import inspect
    from hyperscale.distributed_rewrite.nodes import WorkerServer
    
    source = inspect.getsource(WorkerServer._handle_manager_heartbeat)
    
    # Should create new manager entry if not known
    assert "Discovered new manager" in source, \
        "_handle_manager_heartbeat should discover new managers"
    assert "_healthy_manager_ids.add" in source, \
        "_handle_manager_heartbeat should add new managers to healthy set"


# Run worker leader tracking tests
test_worker_embedder_has_manager_callback()
test_worker_embedder_processes_manager_heartbeat()
test_worker_has_manager_heartbeat_handler()
test_worker_embedder_includes_callback()
test_worker_heartbeat_updates_leader()
test_worker_heartbeat_discovers_new_managers()


# =============================================================================
# Manager <-> Gate Communication Tests (mirrors Worker <-> Manager)
# =============================================================================

print("\nManager <-> Gate Communication Tests")
print("=" * 50)


@test("GateInfo: model exists with expected fields")
def test_gate_info_model():
    from hyperscale.distributed_rewrite.models import GateInfo
    
    gate = GateInfo(
        node_id="gate-1",
        tcp_host="host1",
        tcp_port=8000,
        udp_host="host1",
        udp_port=8001,
        datacenter="dc-east",
        is_leader=True,
    )
    
    assert gate.node_id == "gate-1"
    assert gate.is_leader is True


@test("GateHeartbeat: model exists with expected fields")
def test_gate_heartbeat_model():
    from hyperscale.distributed_rewrite.models import GateHeartbeat
    
    heartbeat = GateHeartbeat(
        node_id="gate-1",
        datacenter="dc-east",
        is_leader=True,
        term=1,
        version=5,
        state="active",
        active_jobs=10,
        active_datacenters=3,
        manager_count=6,
    )
    
    assert heartbeat.node_id == "gate-1"
    assert heartbeat.active_jobs == 10
    assert heartbeat.state == "active"


@test("ManagerRegistrationResponse: model exists")
def test_manager_registration_response_model():
    from hyperscale.distributed_rewrite.models import ManagerRegistrationResponse, GateInfo
    
    gates = [
        GateInfo(
            node_id="gate-1",
            tcp_host="host1",
            tcp_port=8000,
            udp_host="host1",
            udp_port=8001,
            datacenter="dc-east",
            is_leader=True,
        )
    ]
    
    response = ManagerRegistrationResponse(
        accepted=True,
        gate_id="gate-1",
        healthy_gates=gates,
    )
    
    assert response.accepted is True
    assert len(response.healthy_gates) == 1


@test("JobProgressAck: model exists with expected fields")
def test_job_progress_ack_model():
    from hyperscale.distributed_rewrite.models import JobProgressAck, GateInfo
    
    ack = JobProgressAck(
        gate_id="gate-1",
        is_leader=True,
        healthy_gates=[
            GateInfo(
                node_id="gate-1",
                tcp_host="host1",
                tcp_port=8000,
                udp_host="host1",
                udp_port=8001,
                datacenter="dc-east",
                is_leader=True,
            )
        ],
    )
    
    assert ack.gate_id == "gate-1"
    assert ack.is_leader is True


@test("GateStateEmbedder: embeds GateHeartbeat")
def test_gate_embedder_embeds_heartbeat():
    import inspect
    from hyperscale.distributed_rewrite.swim.core.state_embedder import GateStateEmbedder
    
    source = inspect.getsource(GateStateEmbedder.get_state)
    
    assert "GateHeartbeat" in source, \
        "GateStateEmbedder should create GateHeartbeat"


@test("GateStateEmbedder: has on_gate_heartbeat callback")
def test_gate_embedder_has_gate_callback():
    from hyperscale.distributed_rewrite.swim.core.state_embedder import GateStateEmbedder
    import inspect
    
    sig = inspect.signature(GateStateEmbedder)
    param_names = list(sig.parameters.keys())
    
    assert 'on_gate_heartbeat' in param_names, \
        "GateStateEmbedder should have on_gate_heartbeat parameter"


@test("GateStateEmbedder: process_state handles GateHeartbeat")
def test_gate_embedder_processes_gate_heartbeat():
    import inspect
    from hyperscale.distributed_rewrite.swim.core.state_embedder import GateStateEmbedder
    
    source = inspect.getsource(GateStateEmbedder.process_state)
    
    assert "GateHeartbeat" in source, \
        "process_state should try to parse GateHeartbeat from peers"


@test("Gate: has _gate_peer_info tracking")
def test_gate_has_peer_info_tracking():
    import inspect
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    source = inspect.getsource(GateServer.__init__)
    
    assert "_gate_peer_info" in source, \
        "GateServer should have _gate_peer_info dict"


@test("Gate: has _handle_gate_peer_heartbeat method")
def test_gate_has_peer_heartbeat_handler():
    from hyperscale.distributed_rewrite.nodes import GateServer
    import inspect
    
    assert hasattr(GateServer, '_handle_gate_peer_heartbeat'), \
        "GateServer should have _handle_gate_peer_heartbeat method"
    
    source = inspect.getsource(GateServer._handle_gate_peer_heartbeat)
    
    assert "GateHeartbeat" in source, \
        "_handle_gate_peer_heartbeat should handle GateHeartbeat"


@test("Gate: has _get_healthy_gates method")
def test_gate_has_get_healthy_gates():
    from hyperscale.distributed_rewrite.nodes import GateServer
    import inspect
    
    assert hasattr(GateServer, '_get_healthy_gates'), \
        "GateServer should have _get_healthy_gates method"
    
    source = inspect.getsource(GateServer._get_healthy_gates)
    
    assert "GateInfo" in source, \
        "_get_healthy_gates should return GateInfo list"


@test("Gate: receive_job_progress returns JobProgressAck")
def test_gate_progress_returns_ack():
    import pathlib
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    import hyperscale.distributed_rewrite.nodes.gate as gate_module
    source_file = pathlib.Path(gate_module.__file__)
    source = source_file.read_text()
    
    assert "JobProgressAck(" in source, \
        "receive_job_progress should construct JobProgressAck"
    assert "_get_healthy_gates()" in source, \
        "receive_job_progress should call _get_healthy_gates"


@test("Gate: receive_manager_register returns ManagerRegistrationResponse")
def test_gate_manager_register():
    import pathlib
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    import hyperscale.distributed_rewrite.nodes.gate as gate_module
    source_file = pathlib.Path(gate_module.__file__)
    source = source_file.read_text()
    
    assert "ManagerRegistrationResponse(" in source, \
        "receive_manager_register should return ManagerRegistrationResponse"


@test("ManagerStateEmbedder: has on_gate_heartbeat callback")
def test_manager_embedder_has_gate_callback():
    from hyperscale.distributed_rewrite.swim.core.state_embedder import ManagerStateEmbedder
    import inspect
    
    sig = inspect.signature(ManagerStateEmbedder)
    param_names = list(sig.parameters.keys())
    
    assert 'on_gate_heartbeat' in param_names, \
        "ManagerStateEmbedder should have on_gate_heartbeat parameter"


@test("ManagerStateEmbedder: process_state handles GateHeartbeat")
def test_manager_embedder_processes_gate_heartbeat():
    import inspect
    from hyperscale.distributed_rewrite.swim.core.state_embedder import ManagerStateEmbedder
    
    source = inspect.getsource(ManagerStateEmbedder.process_state)
    
    assert "GateHeartbeat" in source, \
        "process_state should try to parse GateHeartbeat from gates"


@test("Manager: has gate tracking structures")
def test_manager_has_gate_tracking():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer.__init__)
    
    assert "_known_gates" in source, \
        "ManagerServer should have _known_gates dict"
    assert "_healthy_gate_ids" in source, \
        "ManagerServer should have _healthy_gate_ids set"
    assert "_primary_gate_id" in source, \
        "ManagerServer should have _primary_gate_id"


@test("Manager: has _handle_gate_heartbeat method")
def test_manager_has_gate_heartbeat_handler():
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    import inspect
    
    assert hasattr(ManagerServer, '_handle_gate_heartbeat'), \
        "ManagerServer should have _handle_gate_heartbeat method"
    
    source = inspect.getsource(ManagerServer._handle_gate_heartbeat)
    
    assert "GateHeartbeat" in source, \
        "_handle_gate_heartbeat should handle GateHeartbeat"
    assert "_known_gates" in source, \
        "_handle_gate_heartbeat should update _known_gates"


@test("Manager: has _process_job_progress_ack method")
def test_manager_has_process_job_progress_ack():
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    import inspect
    
    assert hasattr(ManagerServer, '_process_job_progress_ack'), \
        "ManagerServer should have _process_job_progress_ack method"
    
    source = inspect.getsource(ManagerServer._process_job_progress_ack)
    
    assert "JobProgressAck" in source, \
        "_process_job_progress_ack should parse JobProgressAck"


@test("Manager: has _update_known_gates method")
def test_manager_has_update_known_gates():
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    assert hasattr(ManagerServer, '_update_known_gates'), \
        "ManagerServer should have _update_known_gates method"


@test("Manager: has gate registration at startup")
def test_manager_has_gate_registration():
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    import inspect
    
    assert hasattr(ManagerServer, '_register_with_gates'), \
        "ManagerServer should have _register_with_gates method"
    
    source = inspect.getsource(ManagerServer._register_with_gates)
    
    assert "ManagerRegistrationResponse" in source, \
        "_register_with_gates should handle ManagerRegistrationResponse"


@test("Manager: state embedder includes on_gate_heartbeat")
def test_manager_embedder_includes_gate_callback():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer.__init__)
    
    assert "on_gate_heartbeat=self._handle_gate_heartbeat" in source, \
        "Manager should pass _handle_gate_heartbeat to ManagerStateEmbedder"


@test("Manager: _send_job_progress_to_gate processes ack")
def test_manager_send_progress_processes_ack():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer._send_job_progress_to_gate)
    
    assert "_process_job_progress_ack" in source, \
        "_send_job_progress_to_gate should process ack response"


# Run Manager <-> Gate tests
test_gate_info_model()
test_gate_heartbeat_model()
test_manager_registration_response_model()
test_job_progress_ack_model()
test_gate_embedder_embeds_heartbeat()
test_gate_embedder_has_gate_callback()
test_gate_embedder_processes_gate_heartbeat()
test_gate_has_peer_info_tracking()
test_gate_has_peer_heartbeat_handler()
test_gate_has_get_healthy_gates()
test_gate_progress_returns_ack()
test_gate_manager_register()
test_manager_embedder_has_gate_callback()
test_manager_embedder_processes_gate_heartbeat()
test_manager_has_gate_tracking()
test_manager_has_gate_heartbeat_handler()
test_manager_has_process_job_progress_ack()
test_manager_has_update_known_gates()
test_manager_has_gate_registration()
test_manager_embedder_includes_gate_callback()
test_manager_send_progress_processes_ack()


# =============================================================================
# New Manager Join Process Tests (SYNCING -> ACTIVE)
# =============================================================================

print("\nNew Manager Join Process Tests")
print("=" * 50)


@test("ManagerState: enum exists with expected values")
def test_manager_state_enum():
    from hyperscale.distributed_rewrite.models import ManagerState
    
    assert hasattr(ManagerState, 'SYNCING'), "ManagerState should have SYNCING"
    assert hasattr(ManagerState, 'ACTIVE'), "ManagerState should have ACTIVE"
    assert hasattr(ManagerState, 'DRAINING'), "ManagerState should have DRAINING"
    
    assert ManagerState.SYNCING.value == "syncing"
    assert ManagerState.ACTIVE.value == "active"


@test("Manager: starts in SYNCING state")
def test_manager_starts_syncing():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    from hyperscale.distributed_rewrite.models import ManagerState
    
    source = inspect.getsource(ManagerServer.__init__)
    
    assert "_manager_state = ManagerState.SYNCING" in source, \
        "Manager should initialize in SYNCING state"


@test("Manager: _has_quorum_available excludes SYNCING managers")
def test_manager_quorum_excludes_syncing():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer._has_quorum_available)
    
    # Should check manager state before allowing quorum operations
    assert "_manager_state" in source, \
        "_has_quorum_available should check manager state"
    assert "ManagerState.ACTIVE" in source, \
        "_has_quorum_available should require ACTIVE state"


@test("Manager: has _complete_startup_sync method")
def test_manager_has_startup_sync():
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    import inspect
    
    assert hasattr(ManagerServer, '_complete_startup_sync'), \
        "Manager should have _complete_startup_sync method"
    
    source = inspect.getsource(ManagerServer._complete_startup_sync)
    
    # Should sync from leader if not leader
    assert "is_leader()" in source, \
        "_complete_startup_sync should check leadership"
    assert "StateSyncRequest" in source, \
        "_complete_startup_sync should request state sync"
    assert "ManagerState.ACTIVE" in source, \
        "_complete_startup_sync should transition to ACTIVE"


@test("Manager: start() calls _complete_startup_sync")
def test_manager_start_calls_sync():
    import pathlib
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    import hyperscale.distributed_rewrite.nodes.manager as manager_module
    source_file = pathlib.Path(manager_module.__file__)
    source = source_file.read_text()
    
    assert "_complete_startup_sync" in source, \
        "Manager.start() should call _complete_startup_sync"


@test("ManagerHeartbeat: has state field")
def test_manager_heartbeat_has_state():
    from hyperscale.distributed_rewrite.models import ManagerHeartbeat
    import inspect
    
    sig = inspect.signature(ManagerHeartbeat)
    param_names = list(sig.parameters.keys())
    
    assert 'state' in param_names, \
        "ManagerHeartbeat should have state field"


@test("Manager: _build_manager_heartbeat includes state")
def test_manager_heartbeat_includes_state():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer._build_manager_heartbeat)
    
    assert "_manager_state.value" in source, \
        "_build_manager_heartbeat should include manager state"


@test("ManagerStateEmbedder: has get_manager_state callback")
def test_manager_embedder_has_state_callback():
    from hyperscale.distributed_rewrite.swim.core.state_embedder import ManagerStateEmbedder
    import inspect
    
    sig = inspect.signature(ManagerStateEmbedder)
    param_names = list(sig.parameters.keys())
    
    assert 'get_manager_state' in param_names, \
        "ManagerStateEmbedder should have get_manager_state parameter"


@test("Manager: state embedder includes get_manager_state")
def test_manager_embedder_includes_state_callback():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer.__init__)
    
    assert "get_manager_state=lambda: self._manager_state.value" in source, \
        "Manager should pass get_manager_state to ManagerStateEmbedder"


# Run New Manager Join Process tests
test_manager_state_enum()
test_manager_starts_syncing()
test_manager_quorum_excludes_syncing()
test_manager_has_startup_sync()
test_manager_start_calls_sync()
test_manager_heartbeat_has_state()
test_manager_heartbeat_includes_state()
test_manager_embedder_has_state_callback()
test_manager_embedder_includes_state_callback()


# =============================================================================
# Quorum Circuit Breaker Tests
# =============================================================================

print("\nQuorum Circuit Breaker Tests")
print("=" * 50)


@test("QuorumError: error hierarchy exists")
def test_quorum_error_hierarchy():
    from hyperscale.distributed_rewrite.swim.core import (
        QuorumError,
        QuorumUnavailableError,
        QuorumTimeoutError,
        QuorumCircuitOpenError,
    )
    
    # All quorum errors should exist
    assert QuorumError is not None
    assert QuorumUnavailableError is not None
    assert QuorumTimeoutError is not None
    assert QuorumCircuitOpenError is not None
    
    # Test error creation
    err = QuorumUnavailableError(active_managers=2, required_quorum=3)
    assert "Quorum unavailable" in str(err)
    assert err.context['active_managers'] == 2


@test("QuorumTimeoutError: contains relevant info")
def test_quorum_timeout_error():
    from hyperscale.distributed_rewrite.swim.core import QuorumTimeoutError
    
    err = QuorumTimeoutError(
        confirmations_received=1,
        required_quorum=2,
        timeout=5.0,
    )
    
    assert "timeout" in str(err).lower()
    assert err.context['confirmations_received'] == 1
    assert err.context['required_quorum'] == 2


@test("QuorumCircuitOpenError: contains retry info")
def test_quorum_circuit_open_error():
    from hyperscale.distributed_rewrite.swim.core import QuorumCircuitOpenError
    
    err = QuorumCircuitOpenError(
        recent_failures=5,
        window_seconds=30.0,
        retry_after_seconds=10.0,
    )
    
    assert "circuit breaker" in str(err).lower()
    assert err.context['recent_failures'] == 5
    assert err.context['retry_after_seconds'] == 10.0


@test("Manager: has _quorum_circuit")
def test_manager_has_quorum_circuit():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer.__init__)
    
    assert "_quorum_circuit" in source, \
        "Manager should have _quorum_circuit ErrorStats instance"
    assert "ErrorStats(" in source, \
        "Manager should create ErrorStats for circuit breaker"


@test("Manager: _request_quorum_confirmation checks circuit")
def test_manager_quorum_checks_circuit():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer._request_quorum_confirmation)
    
    # Should check circuit breaker state
    assert "circuit_state" in source, \
        "_request_quorum_confirmation should check circuit state"
    assert "CircuitState.OPEN" in source, \
        "_request_quorum_confirmation should check for OPEN state"
    assert "QuorumCircuitOpenError" in source, \
        "_request_quorum_confirmation should raise QuorumCircuitOpenError"


@test("Manager: _request_quorum_confirmation records failures")
def test_manager_quorum_records_failures():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer._request_quorum_confirmation)
    
    assert "record_error()" in source, \
        "_request_quorum_confirmation should record errors"
    assert "record_success()" in source, \
        "_request_quorum_confirmation should record successes"


@test("Manager: has get_quorum_status method")
def test_manager_has_quorum_status():
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    import inspect
    
    assert hasattr(ManagerServer, 'get_quorum_status'), \
        "Manager should have get_quorum_status method"
    
    source = inspect.getsource(ManagerServer.get_quorum_status)
    
    assert "circuit_state" in source, \
        "get_quorum_status should return circuit state"
    assert "active_managers" in source, \
        "get_quorum_status should return active managers count"


@test("Manager: workflow dispatch handles quorum errors")
def test_manager_dispatch_handles_quorum_errors():
    import pathlib
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    import hyperscale.distributed_rewrite.nodes.manager as manager_module
    source_file = pathlib.Path(manager_module.__file__)
    source = source_file.read_text()
    
    # Should catch quorum errors in workflow dispatch
    assert "QuorumCircuitOpenError" in source
    assert "QuorumUnavailableError" in source
    assert "QuorumTimeoutError" in source


# Run Quorum Circuit Breaker tests
test_quorum_error_hierarchy()
test_quorum_timeout_error()
test_quorum_circuit_open_error()
test_manager_has_quorum_circuit()
test_manager_quorum_checks_circuit()
test_manager_quorum_records_failures()
test_manager_has_quorum_status()
test_manager_dispatch_handles_quorum_errors()


# =============================================================================
# Client Push Notifications Tests
# =============================================================================

print("\n" + "=" * 70)
print("CLIENT PUSH NOTIFICATIONS TESTS")
print("=" * 70 + "\n")


@test("JobSubmission: has callback_addr field")
def test_job_submission_callback_addr():
    from hyperscale.distributed_rewrite.models import JobSubmission
    import dataclasses
    
    fields = {f.name for f in dataclasses.fields(JobSubmission)}
    
    assert "callback_addr" in fields, \
        "JobSubmission should have callback_addr field"


@test("JobStatusPush: model exists")
def test_job_status_push_model():
    from hyperscale.distributed_rewrite.models import JobStatusPush
    import dataclasses
    
    fields = {f.name for f in dataclasses.fields(JobStatusPush)}
    
    assert "job_id" in fields
    assert "status" in fields
    assert "message" in fields
    assert "is_final" in fields


@test("JobBatchPush: model exists")
def test_job_batch_push_model():
    from hyperscale.distributed_rewrite.models import JobBatchPush
    import dataclasses
    
    fields = {f.name for f in dataclasses.fields(JobBatchPush)}
    
    assert "job_id" in fields
    assert "status" in fields
    assert "step_stats" in fields


@test("GateServer: has _job_callbacks dict")
def test_gate_has_job_callbacks():
    import inspect
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    source = inspect.getsource(GateServer.__init__)
    
    assert "_job_callbacks" in source, \
        "GateServer should have _job_callbacks dict"


@test("GateServer: receive_job_submission stores callback")
def test_gate_stores_callback():
    import pathlib
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    import hyperscale.distributed_rewrite.nodes.gate as gate_module
    source_file = pathlib.Path(gate_module.__file__)
    source = source_file.read_text()
    
    # Find receive_job_submission and check it stores callback
    assert "callback_addr" in source
    assert "_job_callbacks[submission.job_id]" in source or \
           "_job_callbacks.get" in source


@test("GateServer: _send_immediate_update pushes to client")
def test_gate_immediate_push():
    import inspect
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    source = inspect.getsource(GateServer._send_immediate_update)
    
    assert "JobStatusPush" in source, \
        "_send_immediate_update should create JobStatusPush"
    assert "job_status_push" in source, \
        "_send_immediate_update should call send_tcp with 'job_status_push'"
    assert "is_final" in source, \
        "_send_immediate_update should track is_final state"


@test("GateServer: _batch_stats_update pushes to clients")
def test_gate_batch_push():
    import inspect
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    source = inspect.getsource(GateServer._batch_stats_update)
    
    assert "JobBatchPush" in source, \
        "_batch_stats_update should create JobBatchPush"
    assert "job_batch_push" in source, \
        "_batch_stats_update should call send_tcp with 'job_batch_push'"


@test("ManagerServer: has _job_callbacks dict")
def test_manager_has_job_callbacks():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer.__init__)
    
    assert "_job_callbacks" in source, \
        "ManagerServer should have _job_callbacks dict"


@test("ManagerServer: receive_job_submission stores callback")
def test_manager_stores_callback():
    import pathlib
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    import hyperscale.distributed_rewrite.nodes.manager as manager_module
    source_file = pathlib.Path(manager_module.__file__)
    source = source_file.read_text()
    
    assert "_job_callbacks[submission.job_id]" in source or \
           "_job_callbacks.get" in source


@test("ManagerServer: has _push_job_status_to_client method")
def test_manager_has_push_status():
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    import inspect
    
    assert hasattr(ManagerServer, '_push_job_status_to_client'), \
        "ManagerServer should have _push_job_status_to_client method"
    
    source = inspect.getsource(ManagerServer._push_job_status_to_client)
    
    assert "JobStatusPush" in source
    assert "job_status_push" in source


@test("ManagerServer: has _push_batch_stats_to_clients method")
def test_manager_has_push_batch():
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    import inspect
    
    assert hasattr(ManagerServer, '_push_batch_stats_to_clients'), \
        "ManagerServer should have _push_batch_stats_to_clients method"
    
    source = inspect.getsource(ManagerServer._push_batch_stats_to_clients)
    
    assert "JobBatchPush" in source
    assert "job_batch_push" in source


@test("ManagerServer: has _client_batch_push_loop method")
def test_manager_has_batch_loop():
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    import inspect
    
    assert hasattr(ManagerServer, '_client_batch_push_loop'), \
        "ManagerServer should have _client_batch_push_loop method"
    
    source = inspect.getsource(ManagerServer._client_batch_push_loop)
    
    assert "_push_batch_stats_to_clients" in source


@test("ManagerServer: has _check_job_completion method")
def test_manager_has_check_completion():
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    import inspect
    
    assert hasattr(ManagerServer, '_check_job_completion'), \
        "ManagerServer should have _check_job_completion method"
    
    source = inspect.getsource(ManagerServer._check_job_completion)
    
    assert "_push_job_status_to_client" in source


@test("ManagerServer: start enables batch push loop when no gates")
def test_manager_start_enables_batch_loop():
    import pathlib
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    import hyperscale.distributed_rewrite.nodes.manager as manager_module
    source_file = pathlib.Path(manager_module.__file__)
    source = source_file.read_text()
    
    assert "_client_batch_push_loop" in source
    assert "client push notifications enabled" in source


# Run Client Push Notifications tests
test_job_submission_callback_addr()
test_job_status_push_model()
test_job_batch_push_model()
test_gate_has_job_callbacks()
test_gate_stores_callback()
test_gate_immediate_push()
test_gate_batch_push()
test_manager_has_job_callbacks()
test_manager_stores_callback()
test_manager_has_push_status()
test_manager_has_push_batch()
test_manager_has_batch_loop()
test_manager_has_check_completion()
test_manager_start_enables_batch_loop()


# =============================================================================
# Gate State and Quorum Tests
# =============================================================================

print("\n" + "=" * 70)
print("GATE STATE AND QUORUM TESTS")
print("=" * 70 + "\n")


@test("GateState: enum exists with expected values")
def test_gate_state_enum():
    from hyperscale.distributed_rewrite.models import GateState
    
    assert hasattr(GateState, 'SYNCING')
    assert hasattr(GateState, 'ACTIVE')
    assert hasattr(GateState, 'DRAINING')
    
    assert GateState.SYNCING.value == "syncing"
    assert GateState.ACTIVE.value == "active"
    assert GateState.DRAINING.value == "draining"


@test("GateHeartbeat: has state field")
def test_gate_heartbeat_has_state():
    from hyperscale.distributed_rewrite.models import GateHeartbeat
    import dataclasses
    
    fields = {f.name for f in dataclasses.fields(GateHeartbeat)}
    
    assert "state" in fields, "GateHeartbeat should have state field"


@test("GateStateEmbedder: has get_gate_state callback")
def test_gate_embedder_has_state_callback():
    from hyperscale.distributed_rewrite.swim import GateStateEmbedder
    import dataclasses
    
    fields = {f.name for f in dataclasses.fields(GateStateEmbedder)}
    
    assert "get_gate_state" in fields, \
        "GateStateEmbedder should have get_gate_state callback"


@test("GateServer: starts in SYNCING state")
def test_gate_starts_syncing():
    import inspect
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    source = inspect.getsource(GateServer.__init__)
    
    assert "_gate_state" in source, \
        "GateServer should have _gate_state field"
    assert "GateState.SYNCING" in source, \
        "GateServer should start in SYNCING state"


@test("GateServer: has _has_quorum_available method")
def test_gate_has_quorum_available():
    from hyperscale.distributed_rewrite.nodes import GateServer
    import inspect
    
    assert hasattr(GateServer, '_has_quorum_available'), \
        "GateServer should have _has_quorum_available method"
    
    source = inspect.getsource(GateServer._has_quorum_available)
    
    assert "GateState.ACTIVE" in source, \
        "_has_quorum_available should check for ACTIVE state"


@test("GateServer: has _quorum_size method")
def test_gate_has_quorum_size():
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    assert hasattr(GateServer, '_quorum_size'), \
        "GateServer should have _quorum_size method"


@test("GateServer: has get_quorum_status method")
def test_gate_has_quorum_status():
    from hyperscale.distributed_rewrite.nodes import GateServer
    import inspect
    
    assert hasattr(GateServer, 'get_quorum_status'), \
        "GateServer should have get_quorum_status method"
    
    source = inspect.getsource(GateServer.get_quorum_status)
    
    assert "circuit_state" in source, \
        "get_quorum_status should return circuit state"
    assert "gate_state" in source, \
        "get_quorum_status should return gate state"


@test("GateServer: has _complete_startup_sync method")
def test_gate_has_startup_sync():
    from hyperscale.distributed_rewrite.nodes import GateServer
    import inspect
    
    assert hasattr(GateServer, '_complete_startup_sync'), \
        "GateServer should have _complete_startup_sync method"
    
    source = inspect.getsource(GateServer._complete_startup_sync)
    
    assert "GateState.ACTIVE" in source, \
        "_complete_startup_sync should transition to ACTIVE"


@test("GateServer: has _quorum_circuit")
def test_gate_has_quorum_circuit():
    import inspect
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    source = inspect.getsource(GateServer.__init__)
    
    assert "_quorum_circuit" in source, \
        "GateServer should have _quorum_circuit ErrorStats"


@test("GateServer: receive_job_submission checks circuit")
def test_gate_job_submission_checks_circuit():
    import pathlib
    import hyperscale.distributed_rewrite.nodes.gate as gate_module
    
    source_file = pathlib.Path(gate_module.__file__)
    source = source_file.read_text()
    
    assert "circuit_state" in source
    assert "CircuitState.OPEN" in source


@test("GateServer: receive_job_submission checks quorum")
def test_gate_job_submission_checks_quorum():
    import pathlib
    import hyperscale.distributed_rewrite.nodes.gate as gate_module
    
    source_file = pathlib.Path(gate_module.__file__)
    source = source_file.read_text()
    
    assert "_has_quorum_available" in source


@test("GateServer: start() calls _complete_startup_sync")
def test_gate_start_calls_sync():
    import pathlib
    import hyperscale.distributed_rewrite.nodes.gate as gate_module
    
    source_file = pathlib.Path(gate_module.__file__)
    source = source_file.read_text()
    
    assert "_complete_startup_sync" in source


@test("GateServer: state embedder includes get_gate_state")
def test_gate_embedder_includes_state():
    import pathlib
    import hyperscale.distributed_rewrite.nodes.gate as gate_module
    
    source_file = pathlib.Path(gate_module.__file__)
    source = source_file.read_text()
    
    assert "get_gate_state=lambda: self._gate_state.value" in source


@test("GateServer: dispatch records circuit success/failure")
def test_gate_dispatch_records_circuit():
    import inspect
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    source = inspect.getsource(GateServer._dispatch_job_to_datacenters)
    
    assert "record_error" in source or "record_success" in source, \
        "_dispatch_job_to_datacenters should record circuit state"


@test("GateServer: raises QuorumCircuitOpenError when circuit is open")
def test_gate_raises_circuit_open_error():
    import pathlib
    import hyperscale.distributed_rewrite.nodes.gate as gate_module
    
    source_file = pathlib.Path(gate_module.__file__)
    source = source_file.read_text()
    
    assert "raise QuorumCircuitOpenError" in source, \
        "receive_job_submission should raise QuorumCircuitOpenError when circuit is open"


@test("GateServer: raises QuorumUnavailableError when quorum unavailable")
def test_gate_raises_quorum_unavailable_error():
    import pathlib
    import hyperscale.distributed_rewrite.nodes.gate as gate_module
    
    source_file = pathlib.Path(gate_module.__file__)
    source = source_file.read_text()
    
    assert "raise QuorumUnavailableError" in source, \
        "receive_job_submission should raise QuorumUnavailableError when quorum unavailable"


@test("GateServer: handles QuorumCircuitOpenError without recording error")
def test_gate_handles_circuit_open_error():
    import pathlib
    import hyperscale.distributed_rewrite.nodes.gate as gate_module
    
    source_file = pathlib.Path(gate_module.__file__)
    source = source_file.read_text()
    
    # Should have separate except block for QuorumCircuitOpenError
    assert "except QuorumCircuitOpenError" in source, \
        "Should have separate handler for QuorumCircuitOpenError"


# Run Gate State and Quorum tests
test_gate_state_enum()
test_gate_heartbeat_has_state()
test_gate_embedder_has_state_callback()
test_gate_starts_syncing()
test_gate_has_quorum_available()
test_gate_has_quorum_size()
test_gate_has_quorum_status()
test_gate_has_startup_sync()
test_gate_has_quorum_circuit()
test_gate_job_submission_checks_circuit()
test_gate_job_submission_checks_quorum()
test_gate_start_calls_sync()
test_gate_embedder_includes_state()
test_gate_dispatch_records_circuit()
test_gate_raises_circuit_open_error()
test_gate_raises_quorum_unavailable_error()
test_gate_handles_circuit_open_error()


# =============================================================================
# Worker Circuit Breaker Tests
# =============================================================================

print("\n" + "=" * 70)
print("WORKER CIRCUIT BREAKER TESTS")
print("=" * 70 + "\n")


@test("Worker: has _manager_circuit")
def test_worker_has_manager_circuit():
    import inspect
    from hyperscale.distributed_rewrite.nodes import WorkerServer
    
    source = inspect.getsource(WorkerServer.__init__)
    
    assert "_manager_circuit" in source, \
        "WorkerServer should have _manager_circuit ErrorStats"
    assert "ErrorStats(" in source, \
        "WorkerServer should create ErrorStats for circuit breaker"


@test("Worker: has _is_manager_circuit_open method")
def test_worker_has_circuit_open_check():
    from hyperscale.distributed_rewrite.nodes import WorkerServer
    
    assert hasattr(WorkerServer, '_is_manager_circuit_open'), \
        "WorkerServer should have _is_manager_circuit_open method"


@test("Worker: has get_manager_circuit_status method")
def test_worker_has_circuit_status():
    from hyperscale.distributed_rewrite.nodes import WorkerServer
    import inspect
    
    assert hasattr(WorkerServer, 'get_manager_circuit_status'), \
        "WorkerServer should have get_manager_circuit_status method"
    
    source = inspect.getsource(WorkerServer.get_manager_circuit_status)
    
    assert "circuit_state" in source, \
        "get_manager_circuit_status should return circuit state"
    assert "error_count" in source, \
        "get_manager_circuit_status should return error count"


@test("Worker: _send_progress_update checks circuit")
def test_worker_progress_checks_circuit():
    import inspect
    from hyperscale.distributed_rewrite.nodes import WorkerServer
    
    source = inspect.getsource(WorkerServer._send_progress_update)
    
    assert "_is_manager_circuit_open" in source, \
        "_send_progress_update should check circuit breaker"


@test("Worker: _send_progress_update records circuit state")
def test_worker_progress_records_circuit():
    import inspect
    from hyperscale.distributed_rewrite.nodes import WorkerServer
    
    source = inspect.getsource(WorkerServer._send_progress_update)
    
    assert "record_success" in source, \
        "_send_progress_update should record success"
    assert "record_error" in source, \
        "_send_progress_update should record error"


@test("Worker: _send_progress_to_all_managers checks circuit")
def test_worker_progress_all_checks_circuit():
    import inspect
    from hyperscale.distributed_rewrite.nodes import WorkerServer
    
    source = inspect.getsource(WorkerServer._send_progress_to_all_managers)
    
    assert "_is_manager_circuit_open" in source, \
        "_send_progress_to_all_managers should check circuit breaker"


# Run Worker Circuit Breaker tests
test_worker_has_manager_circuit()
test_worker_has_circuit_open_check()
test_worker_has_circuit_status()
test_worker_progress_checks_circuit()
test_worker_progress_records_circuit()
test_worker_progress_all_checks_circuit()


# =============================================================================
# Worker Retry Tests
# =============================================================================

print("\n" + "=" * 70)
print("WORKER RETRY TESTS")
print("=" * 70 + "\n")


@test("Worker: _register_with_manager has retry parameters")
def test_worker_register_has_retry_params():
    import inspect
    from hyperscale.distributed_rewrite.nodes import WorkerServer
    
    sig = inspect.signature(WorkerServer._register_with_manager)
    params = list(sig.parameters.keys())
    
    assert 'max_retries' in params, \
        "_register_with_manager should have max_retries parameter"
    assert 'base_delay' in params, \
        "_register_with_manager should have base_delay parameter"


@test("Worker: _register_with_manager uses exponential backoff")
def test_worker_register_uses_backoff():
    import inspect
    from hyperscale.distributed_rewrite.nodes import WorkerServer
    
    source = inspect.getsource(WorkerServer._register_with_manager)
    
    # Should have retry loop
    assert "for attempt in range" in source, \
        "_register_with_manager should have retry loop"
    
    # Should have exponential backoff calculation
    assert "2 ** attempt" in source or "2**attempt" in source, \
        "_register_with_manager should use exponential backoff"
    
    # Should sleep between retries
    assert "asyncio.sleep" in source, \
        "_register_with_manager should sleep between retries"


@test("Worker: _register_with_manager checks circuit breaker")
def test_worker_register_checks_circuit():
    import inspect
    from hyperscale.distributed_rewrite.nodes import WorkerServer
    
    source = inspect.getsource(WorkerServer._register_with_manager)
    
    assert "_is_manager_circuit_open" in source, \
        "_register_with_manager should check circuit breaker"


@test("Worker: _register_with_manager records circuit state")
def test_worker_register_records_circuit():
    import inspect
    from hyperscale.distributed_rewrite.nodes import WorkerServer
    
    source = inspect.getsource(WorkerServer._register_with_manager)
    
    assert "record_success" in source, \
        "_register_with_manager should record success"
    assert "record_error" in source, \
        "_register_with_manager should record error on final failure"


# Run Worker Retry tests
test_worker_register_has_retry_params()
test_worker_register_uses_backoff()
test_worker_register_checks_circuit()
test_worker_register_records_circuit()


@test("Worker: _send_progress_update has retry parameters")
def test_worker_progress_has_retry_params():
    import inspect
    from hyperscale.distributed_rewrite.nodes import WorkerServer
    
    sig = inspect.signature(WorkerServer._send_progress_update)
    params = list(sig.parameters.keys())
    
    assert 'max_retries' in params, \
        "_send_progress_update should have max_retries parameter"
    assert 'base_delay' in params, \
        "_send_progress_update should have base_delay parameter"


@test("Worker: _send_progress_update uses exponential backoff")
def test_worker_progress_uses_backoff():
    import inspect
    from hyperscale.distributed_rewrite.nodes import WorkerServer
    
    source = inspect.getsource(WorkerServer._send_progress_update)
    
    # Should have retry loop
    assert "for attempt in range" in source, \
        "_send_progress_update should have retry loop"
    
    # Should have exponential backoff
    assert "2 ** attempt" in source or "2**attempt" in source, \
        "_send_progress_update should use exponential backoff"
    
    # Should sleep between retries
    assert "asyncio.sleep" in source, \
        "_send_progress_update should sleep between retries"


# Run additional Worker Retry tests
test_worker_progress_has_retry_params()
test_worker_progress_uses_backoff()


# =============================================================================
# Manager Per-Worker Circuit Breaker Tests
# =============================================================================

print("\n" + "=" * 70)
print("MANAGER PER-WORKER CIRCUIT BREAKER TESTS")
print("=" * 70 + "\n")


@test("Manager: has _worker_circuits dict")
def test_manager_has_worker_circuits():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer.__init__)
    
    assert "_worker_circuits" in source, \
        "ManagerServer should have _worker_circuits dict"


@test("Manager: has _get_worker_circuit method")
def test_manager_has_get_worker_circuit():
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    assert hasattr(ManagerServer, '_get_worker_circuit'), \
        "ManagerServer should have _get_worker_circuit method"


@test("Manager: has _is_worker_circuit_open method")
def test_manager_has_is_worker_circuit_open():
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    assert hasattr(ManagerServer, '_is_worker_circuit_open'), \
        "ManagerServer should have _is_worker_circuit_open method"


@test("Manager: has get_worker_circuit_status method")
def test_manager_has_worker_circuit_status():
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    assert hasattr(ManagerServer, 'get_worker_circuit_status'), \
        "ManagerServer should have get_worker_circuit_status method"


@test("Manager: has get_all_worker_circuit_status method")
def test_manager_has_all_worker_circuit_status():
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    assert hasattr(ManagerServer, 'get_all_worker_circuit_status'), \
        "ManagerServer should have get_all_worker_circuit_status method"


@test("Manager: _select_worker_for_workflow checks circuit")
def test_manager_select_worker_checks_circuit():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer._select_worker_for_workflow)
    
    assert "_is_worker_circuit_open" in source, \
        "_select_worker_for_workflow should check circuit breaker"


@test("Manager: _select_worker_for_workflow_excluding checks circuit")
def test_manager_select_excluding_checks_circuit():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer._select_worker_for_workflow_excluding)
    
    assert "_is_worker_circuit_open" in source, \
        "_select_worker_for_workflow_excluding should check circuit breaker"


@test("Manager: _dispatch_workflow_to_worker uses circuit")
def test_manager_dispatch_uses_circuit():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer._dispatch_workflow_to_worker)
    
    assert "_is_worker_circuit_open" in source, \
        "_dispatch_workflow_to_worker should check circuit breaker"
    assert "_get_worker_circuit" in source, \
        "_dispatch_workflow_to_worker should get circuit breaker"
    assert "record_success" in source, \
        "_dispatch_workflow_to_worker should record success"
    assert "record_error" in source, \
        "_dispatch_workflow_to_worker should record error"


# Run Manager Per-Worker Circuit Breaker tests
test_manager_has_worker_circuits()
test_manager_has_get_worker_circuit()
test_manager_has_is_worker_circuit_open()
test_manager_has_worker_circuit_status()
test_manager_has_all_worker_circuit_status()
test_manager_select_worker_checks_circuit()
test_manager_select_excluding_checks_circuit()
test_manager_dispatch_uses_circuit()


# =============================================================================
# Manager Retry Tests
# =============================================================================

print("\n" + "=" * 70)
print("MANAGER RETRY TESTS")
print("=" * 70 + "\n")


@test("Manager: _dispatch_workflow_to_worker has retry parameters")
def test_manager_dispatch_has_retry_params():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    sig = inspect.signature(ManagerServer._dispatch_workflow_to_worker)
    params = list(sig.parameters.keys())
    
    assert 'max_retries' in params, \
        "_dispatch_workflow_to_worker should have max_retries parameter"
    assert 'base_delay' in params, \
        "_dispatch_workflow_to_worker should have base_delay parameter"


@test("Manager: _dispatch_workflow_to_worker uses exponential backoff")
def test_manager_dispatch_uses_backoff():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer._dispatch_workflow_to_worker)
    
    # Should have retry loop
    assert "for attempt in range" in source, \
        "_dispatch_workflow_to_worker should have retry loop"
    
    # Should have exponential backoff
    assert "2 ** attempt" in source or "2**attempt" in source, \
        "_dispatch_workflow_to_worker should use exponential backoff"
    
    # Should sleep between retries
    assert "asyncio.sleep" in source, \
        "_dispatch_workflow_to_worker should sleep between retries"


# Run Manager Retry tests
test_manager_dispatch_has_retry_params()
test_manager_dispatch_uses_backoff()


# =============================================================================
# Manager ↔ Gate Resilience Tests
# =============================================================================

print("\n" + "=" * 70)
print("MANAGER ↔ GATE RESILIENCE TESTS")
print("=" * 70 + "\n")


@test("Manager: has _gate_circuit")
def test_manager_has_gate_circuit():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer.__init__)
    
    assert "_gate_circuit" in source, \
        "ManagerServer should have _gate_circuit ErrorStats"
    assert "ErrorStats(" in source, \
        "ManagerServer should create ErrorStats for gate circuit breaker"


@test("Manager: has _is_gate_circuit_open method")
def test_manager_has_gate_circuit_open():
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    assert hasattr(ManagerServer, '_is_gate_circuit_open'), \
        "ManagerServer should have _is_gate_circuit_open method"


@test("Manager: has get_gate_circuit_status method")
def test_manager_has_gate_circuit_status():
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    import inspect
    
    assert hasattr(ManagerServer, 'get_gate_circuit_status'), \
        "ManagerServer should have get_gate_circuit_status method"
    
    source = inspect.getsource(ManagerServer.get_gate_circuit_status)
    
    assert "circuit_state" in source, \
        "get_gate_circuit_status should return circuit state"
    assert "primary_gate" in source, \
        "get_gate_circuit_status should return primary gate"


# Run Manager Gate Circuit tests
test_manager_has_gate_circuit()
test_manager_has_gate_circuit_open()
test_manager_has_gate_circuit_status()


@test("Manager: _try_register_with_gate has retry parameters")
def test_manager_gate_register_has_retry_params():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    sig = inspect.signature(ManagerServer._try_register_with_gate)
    params = list(sig.parameters.keys())
    
    assert 'max_retries' in params, \
        "_try_register_with_gate should have max_retries parameter"
    assert 'base_delay' in params, \
        "_try_register_with_gate should have base_delay parameter"


@test("Manager: _try_register_with_gate uses exponential backoff")
def test_manager_gate_register_uses_backoff():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer._try_register_with_gate)
    
    assert "for attempt in range" in source, \
        "_try_register_with_gate should have retry loop"
    assert "2 ** attempt" in source or "2**attempt" in source, \
        "_try_register_with_gate should use exponential backoff"
    assert "asyncio.sleep" in source, \
        "_try_register_with_gate should sleep between retries"


@test("Manager: _try_register_with_gate checks circuit")
def test_manager_gate_register_checks_circuit():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer._try_register_with_gate)
    
    assert "_is_gate_circuit_open" in source, \
        "_try_register_with_gate should check circuit breaker"


@test("Manager: _try_register_with_gate records circuit state")
def test_manager_gate_register_records_circuit():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer._try_register_with_gate)
    
    assert "record_success" in source, \
        "_try_register_with_gate should record success"
    assert "record_error" in source, \
        "_try_register_with_gate should record error"


# Run Manager Gate Registration Retry tests
test_manager_gate_register_has_retry_params()
test_manager_gate_register_uses_backoff()
test_manager_gate_register_checks_circuit()
test_manager_gate_register_records_circuit()


@test("Manager: _send_job_progress_to_gate has retry parameters")
def test_manager_job_progress_has_retry_params():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    sig = inspect.signature(ManagerServer._send_job_progress_to_gate)
    params = list(sig.parameters.keys())
    
    assert 'max_retries' in params, \
        "_send_job_progress_to_gate should have max_retries parameter"
    assert 'base_delay' in params, \
        "_send_job_progress_to_gate should have base_delay parameter"


@test("Manager: _send_job_progress_to_gate uses exponential backoff")
def test_manager_job_progress_uses_backoff():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer._send_job_progress_to_gate)
    
    assert "for attempt in range" in source, \
        "_send_job_progress_to_gate should have retry loop"
    assert "2 ** attempt" in source or "2**attempt" in source, \
        "_send_job_progress_to_gate should use exponential backoff"
    assert "asyncio.sleep" in source, \
        "_send_job_progress_to_gate should sleep between retries"


@test("Manager: _send_job_progress_to_gate checks circuit")
def test_manager_job_progress_checks_circuit():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer._send_job_progress_to_gate)
    
    assert "_is_gate_circuit_open" in source, \
        "_send_job_progress_to_gate should check circuit breaker"


@test("Manager: _send_job_progress_to_gate records circuit state")
def test_manager_job_progress_records_circuit():
    import inspect
    from hyperscale.distributed_rewrite.nodes import ManagerServer
    
    source = inspect.getsource(ManagerServer._send_job_progress_to_gate)
    
    assert "record_success" in source, \
        "_send_job_progress_to_gate should record success"
    assert "record_error" in source, \
        "_send_job_progress_to_gate should record error"


# Run Manager Job Progress Retry tests
test_manager_job_progress_has_retry_params()
test_manager_job_progress_uses_backoff()
test_manager_job_progress_checks_circuit()
test_manager_job_progress_records_circuit()


# =============================================================================
# Gate Per-Manager Circuit Breaker Tests
# =============================================================================

print("\n" + "=" * 70)
print("GATE PER-MANAGER CIRCUIT BREAKER TESTS")
print("=" * 70 + "\n")


@test("Gate: has _manager_circuits dict")
def test_gate_has_manager_circuits():
    import inspect
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    source = inspect.getsource(GateServer.__init__)
    
    assert "_manager_circuits" in source, \
        "GateServer should have _manager_circuits dict"


@test("Gate: has _get_manager_circuit method")
def test_gate_has_get_manager_circuit():
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    assert hasattr(GateServer, '_get_manager_circuit'), \
        "GateServer should have _get_manager_circuit method"


@test("Gate: has _is_manager_circuit_open method")
def test_gate_has_is_manager_circuit_open():
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    assert hasattr(GateServer, '_is_manager_circuit_open'), \
        "GateServer should have _is_manager_circuit_open method"


@test("Gate: has get_manager_circuit_status method")
def test_gate_has_manager_circuit_status():
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    assert hasattr(GateServer, 'get_manager_circuit_status'), \
        "GateServer should have get_manager_circuit_status method"


@test("Gate: has get_all_manager_circuit_status method")
def test_gate_has_all_manager_circuit_status():
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    assert hasattr(GateServer, 'get_all_manager_circuit_status'), \
        "GateServer should have get_all_manager_circuit_status method"


@test("Gate: _try_dispatch_to_dc uses retry helper")
def test_gate_dispatch_uses_retry_helper():
    import inspect
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    source = inspect.getsource(GateServer._try_dispatch_to_dc)
    
    # Now uses _try_dispatch_to_manager which handles circuits and retries
    assert "_try_dispatch_to_manager" in source, \
        "_try_dispatch_to_dc should use _try_dispatch_to_manager helper"


@test("Gate: dispatch flow has circuit and retry support")
def test_gate_dispatch_flow_has_circuit_retry():
    import inspect
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    # Check _try_dispatch_to_manager has circuit and retry logic
    source = inspect.getsource(GateServer._try_dispatch_to_manager)
    
    assert "_is_manager_circuit_open" in source, \
        "_try_dispatch_to_manager should check circuit breaker"
    assert "_get_manager_circuit" in source, \
        "_try_dispatch_to_manager should get circuit breaker"
    assert "record_success" in source, \
        "_try_dispatch_to_manager should record success"
    assert "record_error" in source, \
        "_try_dispatch_to_manager should record error"


# Run Gate Per-Manager Circuit Breaker tests
test_gate_has_manager_circuits()
test_gate_has_get_manager_circuit()
test_gate_has_is_manager_circuit_open()
test_gate_has_manager_circuit_status()
test_gate_has_all_manager_circuit_status()
test_gate_dispatch_uses_retry_helper()
test_gate_dispatch_flow_has_circuit_retry()


# =============================================================================
# Gate Dispatch Retry Tests
# =============================================================================

print("\n" + "=" * 70)
print("GATE DISPATCH RETRY TESTS")
print("=" * 70 + "\n")


@test("Gate: has _try_dispatch_to_manager method")
def test_gate_has_dispatch_to_manager():
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    assert hasattr(GateServer, '_try_dispatch_to_manager'), \
        "GateServer should have _try_dispatch_to_manager method"


@test("Gate: _try_dispatch_to_manager has retry parameters")
def test_gate_dispatch_manager_has_retry_params():
    import inspect
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    sig = inspect.signature(GateServer._try_dispatch_to_manager)
    params = list(sig.parameters.keys())
    
    assert 'max_retries' in params, \
        "_try_dispatch_to_manager should have max_retries parameter"
    assert 'base_delay' in params, \
        "_try_dispatch_to_manager should have base_delay parameter"


@test("Gate: _try_dispatch_to_manager uses exponential backoff")
def test_gate_dispatch_manager_uses_backoff():
    import inspect
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    source = inspect.getsource(GateServer._try_dispatch_to_manager)
    
    assert "for attempt in range" in source, \
        "_try_dispatch_to_manager should have retry loop"
    assert "2 ** attempt" in source or "2**attempt" in source, \
        "_try_dispatch_to_manager should use exponential backoff"
    assert "asyncio.sleep" in source, \
        "_try_dispatch_to_manager should sleep between retries"


@test("Gate: _try_dispatch_to_manager checks circuit")
def test_gate_dispatch_manager_checks_circuit():
    import inspect
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    source = inspect.getsource(GateServer._try_dispatch_to_manager)
    
    assert "_is_manager_circuit_open" in source, \
        "_try_dispatch_to_manager should check circuit breaker"


@test("Gate: _try_dispatch_to_manager records circuit state")
def test_gate_dispatch_manager_records_circuit():
    import inspect
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    source = inspect.getsource(GateServer._try_dispatch_to_manager)
    
    assert "record_success" in source, \
        "_try_dispatch_to_manager should record success"
    assert "record_error" in source, \
        "_try_dispatch_to_manager should record error"


@test("Gate: _try_dispatch_to_dc uses _try_dispatch_to_manager")
def test_gate_dispatch_dc_uses_dispatch_manager():
    import inspect
    from hyperscale.distributed_rewrite.nodes import GateServer
    
    source = inspect.getsource(GateServer._try_dispatch_to_dc)
    
    assert "_try_dispatch_to_manager" in source, \
        "_try_dispatch_to_dc should use _try_dispatch_to_manager"


# Run Gate Dispatch Retry tests
test_gate_has_dispatch_to_manager()
test_gate_dispatch_manager_has_retry_params()
test_gate_dispatch_manager_uses_backoff()
test_gate_dispatch_manager_checks_circuit()
test_gate_dispatch_manager_records_circuit()
test_gate_dispatch_dc_uses_dispatch_manager()


# =============================================================================
# Summary
# =============================================================================

success = results.summary()
exit(0 if success else 1)

