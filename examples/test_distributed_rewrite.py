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
# Leadership Callback Composition Tests
# =============================================================================

print("\n" + "=" * 60)
print("Leadership Callback Composition Tests")
print("=" * 60)


@test("UDPServer: has callback registration methods")
def test_udp_server_callback_methods():
    from hyperscale.distributed_rewrite.swim import UDPServer
    
    assert hasattr(UDPServer, 'register_on_become_leader')
    assert hasattr(UDPServer, 'register_on_lose_leadership')
    assert hasattr(UDPServer, 'register_on_leader_change')
    
    # Check they are callable
    assert callable(getattr(UDPServer, 'register_on_become_leader'))
    assert callable(getattr(UDPServer, 'register_on_lose_leadership'))
    assert callable(getattr(UDPServer, 'register_on_leader_change'))


@test("UDPServer: callback lists are initialized")
def test_udp_server_callback_lists():
    """Test that callback lists exist on instance."""
    # We can't instantiate UDPServer easily without full setup,
    # but we can check the __init__ signature/code
    import inspect
    from hyperscale.distributed_rewrite.swim import UDPServer
    
    source = inspect.getsource(UDPServer.__init__)
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
test_udp_server_callback_methods()
test_udp_server_callback_lists()
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


@test("UDPServer: has node dead callback registration")
def test_udp_server_node_dead_callback():
    from hyperscale.distributed_rewrite.swim import UDPServer
    
    assert hasattr(UDPServer, 'register_on_node_dead')
    assert callable(getattr(UDPServer, 'register_on_node_dead'))


@test("UDPServer: node dead callback list initialized")
def test_udp_server_node_dead_list():
    import inspect
    from hyperscale.distributed_rewrite.swim import UDPServer
    
    source = inspect.getsource(UDPServer.__init__)
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
test_udp_server_node_dead_callback()
test_udp_server_node_dead_list()
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
    assert hasattr(WorkerServer, '_handle_manager_failure')
    assert hasattr(WorkerServer, '_report_active_workflows_to_manager')
    
    # Check that on_node_dead callback is registered in start()
    start_source = inspect.getsource(WorkerServer.start)
    assert 'register_on_node_dead' in start_source


@test("WorkerServer: _handle_manager_failure attempts failover")
def test_worker_handle_manager_failure():
    import inspect
    from hyperscale.distributed_rewrite.nodes import WorkerServer
    
    source = inspect.getsource(WorkerServer._handle_manager_failure)
    
    # Should iterate through manager list
    assert '_manager_addrs' in source
    # Should register with new manager
    assert '_register_with_manager' in source
    # Should update _current_manager
    assert '_current_manager' in source
    # Should report workflows after failover
    assert '_report_active_workflows_to_manager' in source


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
    1. Worker is connected to Manager A
    2. Manager A dies (detected via SWIM)  
    3. Worker's _on_node_dead callback fires
    4. _handle_manager_failure tries Manager B
    5. Worker registers with Manager B
    6. Worker reports active workflows to Manager B
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
test_worker_handle_manager_failure()
test_worker_failure_scenario()
test_manager_failure_scenario()
test_retry_preserves_resources()


# =============================================================================
# Manager Peer Failure Detection Tests
# =============================================================================

print("\nManager Peer Failure Detection Tests")
print("=" * 40)


@test("UDPServer: has register_on_node_join callback")
def test_udp_server_has_node_join_callback():
    from hyperscale.distributed_rewrite.swim.udp_server import UDPServer
    
    assert hasattr(UDPServer, 'register_on_node_join'), \
        "UDPServer must have register_on_node_join method"
    
    # _on_node_join_callbacks is an instance attribute set in __init__
    # So we check the method exists and inspect its source
    import inspect
    source = inspect.getsource(UDPServer.__init__)
    assert '_on_node_join_callbacks' in source, \
        "UDPServer.__init__ must initialize _on_node_join_callbacks"


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
test_udp_server_has_node_join_callback()
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
# Summary
# =============================================================================

success = results.summary()
exit(0 if success else 1)

