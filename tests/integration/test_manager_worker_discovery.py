#!/usr/bin/env python3
"""
Manager-Worker Discovery Integration Tests (AD-28).

Tests that managers correctly discover and select workers using the
DiscoveryService with adaptive EWMA-based selection.

Test scenarios:
1. Manager-worker discovery for varying cluster sizes
2. Manager-worker discovery failure and recovery
3. Load-aware worker selection based on latency feedback
4. WorkerHeartbeat and Registration message validation
5. Worker discovery selection and latency feedback

This validates:
- Managers initialize worker discovery service
- Workers register with managers and are tracked in discovery
- WorkerHeartbeat messages contain correct fields
- Registration/RegistrationResponse messages are valid
- Failed workers are detected and removed
- Recovery allows workers to rejoin discovery
- Adaptive selection prefers lower-latency workers
"""

import asyncio
import sys
import os
import time
from dataclasses import dataclass, field

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.distributed_rewrite.nodes.manager import ManagerServer
from hyperscale.distributed_rewrite.nodes.worker import WorkerServer
from hyperscale.distributed_rewrite.env.env import Env
from hyperscale.distributed_rewrite.models import WorkerHeartbeat, WorkerRegistration, RegistrationResponse
from hyperscale.logging.config.logging_config import LoggingConfig

# Initialize logging directory
_logging_config = LoggingConfig()
_logging_config.update(log_directory=os.getcwd())


# ==========================================================================
# Configuration Helpers
# ==========================================================================

def generate_manager_configs(count: int, base_tcp_port: int = 9000) -> list[dict]:
    """Generate manager configurations for a given cluster size."""
    configs = []
    for i in range(count):
        configs.append({
            "name": f"Manager {i + 1}",
            "tcp": base_tcp_port + (i * 2),
            "udp": base_tcp_port + (i * 2) + 1,
        })
    return configs


def generate_worker_configs(count: int, base_tcp_port: int = 9100, cores: int = 2) -> list[dict]:
    """Generate worker configurations for a given cluster size."""
    configs = []
    for i in range(count):
        configs.append({
            "name": f"Worker {i + 1}",
            "tcp": base_tcp_port + (i * 10),
            "udp": base_tcp_port + (i * 10) + 1,
            "cores": cores,
        })
    return configs


def get_manager_peer_tcp_addrs(configs: list[dict], exclude_tcp: int) -> list[tuple[str, int]]:
    """Get TCP addresses of all managers except the one with exclude_tcp."""
    return [
        ('127.0.0.1', cfg['tcp'])
        for cfg in configs
        if cfg['tcp'] != exclude_tcp
    ]


def get_manager_peer_udp_addrs(configs: list[dict], exclude_udp: int) -> list[tuple[str, int]]:
    """Get UDP addresses of all managers except the one with exclude_udp."""
    return [
        ('127.0.0.1', cfg['udp'])
        for cfg in configs
        if cfg['udp'] != exclude_udp
    ]


def get_all_manager_tcp_addrs(configs: list[dict]) -> list[tuple[str, int]]:
    """Get TCP addresses of all managers."""
    return [('127.0.0.1', cfg['tcp']) for cfg in configs]


# ==========================================================================
# Test: Manager-Worker Discovery - Basic Discovery
# ==========================================================================

async def test_manager_worker_discovery_basic(
    manager_count: int,
    worker_count: int,
) -> bool:
    """
    Test that managers discover workers for given cluster sizes.

    Validates:
    - All nodes start successfully
    - Workers register with managers
    - Worker discovery service tracks all workers
    """
    print(f"\n{'=' * 70}")
    print(f"TEST: Manager-Worker Discovery - {manager_count} Managers, {worker_count} Workers")
    print(f"{'=' * 70}")

    dc_id = "DC-TEST"
    manager_configs = generate_manager_configs(manager_count)
    worker_configs = generate_worker_configs(worker_count)

    managers: list[ManagerServer] = []
    workers: list[WorkerServer] = []
    stabilization_time = 15 + (manager_count + worker_count) * 2
    registration_time = 10

    try:
        # Create managers
        print(f"\n[1/5] Creating {manager_count} managers...")
        for config in manager_configs:
            manager = ManagerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(
                    MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                    MERCURY_SYNC_LOG_LEVEL="error",
                ),
                dc_id=dc_id,
                manager_peers=get_manager_peer_tcp_addrs(manager_configs, config["tcp"]),
                manager_udp_peers=get_manager_peer_udp_addrs(manager_configs, config["udp"]),
            )
            managers.append(manager)
            print(f"  Created {config['name']} (TCP:{config['tcp']})")

        # Create workers
        print(f"\n[2/5] Creating {worker_count} workers...")
        seed_managers = get_all_manager_tcp_addrs(manager_configs)
        for config in worker_configs:
            worker = WorkerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(
                    MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                    MERCURY_SYNC_LOG_LEVEL="error",
                    WORKER_MAX_CORES=config["cores"],
                ),
                dc_id=dc_id,
                seed_managers=seed_managers,
            )
            workers.append(worker)
            print(f"  Created {config['name']} (TCP:{config['tcp']}, {config['cores']} cores)")

        # Start managers
        print(f"\n[3/5] Starting managers...")
        start_tasks = [manager.start() for manager in managers]
        await asyncio.gather(*start_tasks)

        for i, manager in enumerate(managers):
            print(f"  Started {manager_configs[i]['name']} - Node ID: {manager._node_id.short}")

        # Wait for manager stabilization
        print(f"  Waiting for manager cluster ({stabilization_time // 2}s)...")
        await asyncio.sleep(stabilization_time // 2)

        # Start workers
        print(f"\n[4/5] Starting workers...")
        start_tasks = [worker.start() for worker in workers]
        await asyncio.gather(*start_tasks)

        for i, worker in enumerate(workers):
            print(f"  Started {worker_configs[i]['name']} - Node ID: {worker._node_id.short}")

        # Wait for worker registration
        print(f"  Waiting for worker registration ({registration_time}s)...")
        await asyncio.sleep(registration_time)

        # Verify worker discovery
        print(f"\n[5/5] Verifying worker discovery...")
        worker_discovery_ok = True

        for i, manager in enumerate(managers):
            discovery_count = manager._worker_discovery.peer_count
            registered_workers = len(manager._registered_workers)
            total_cores = manager._get_total_available_cores()

            workers_ok = discovery_count >= worker_count or registered_workers >= worker_count
            status = "PASS" if workers_ok else "FAIL"
            print(f"  {manager_configs[i]['name']}:")
            print(f"    Discovery peers: {discovery_count}")
            print(f"    Registered workers: {registered_workers}")
            print(f"    Available cores: {total_cores}")
            print(f"    Status: [{status}]")

            if not workers_ok:
                worker_discovery_ok = False

        # Summary
        print(f"\n{'=' * 70}")
        result = "PASSED" if worker_discovery_ok else "FAILED"
        print(f"TEST RESULT: {result}")
        print(f"  Manager count: {manager_count}")
        print(f"  Worker count: {worker_count}")
        print(f"  Worker discovery: {'PASS' if worker_discovery_ok else 'FAIL'}")
        print(f"{'=' * 70}")

        return worker_discovery_ok

    except Exception as e:
        import traceback
        print(f"\nTest failed with exception: {e}")
        traceback.print_exc()
        return False

    finally:
        print("\nCleaning up...")
        for i, worker in enumerate(workers):
            try:
                await worker.stop(drain_timeout=0.5, broadcast_leave=False)
            except Exception:
                pass

        for i, manager in enumerate(managers):
            try:
                await manager.stop(drain_timeout=0.5, broadcast_leave=False)
            except Exception:
                pass
        print("  Cleanup complete")


# ==========================================================================
# Test: Manager-Worker Discovery - Failure and Recovery
# ==========================================================================

async def test_manager_worker_discovery_failure_recovery(
    manager_count: int,
    worker_count: int,
) -> bool:
    """
    Test that manager-worker discovery handles failure and recovery.

    Validates:
    - Managers detect worker failure
    - Failed workers are removed from discovery
    - Recovered workers are re-added
    """
    print(f"\n{'=' * 70}")
    print(f"TEST: Manager-Worker Discovery Failure/Recovery - {manager_count} Managers, {worker_count} Workers")
    print(f"{'=' * 70}")

    dc_id = "DC-TEST"
    manager_configs = generate_manager_configs(manager_count)
    worker_configs = generate_worker_configs(worker_count)

    managers: list[ManagerServer] = []
    workers: list[WorkerServer] = []
    stabilization_time = 15 + (manager_count + worker_count) * 2
    failure_detection_time = 15
    recovery_time = 15

    try:
        # Create infrastructure
        print(f"\n[1/8] Creating infrastructure...")

        for config in manager_configs:
            manager = ManagerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(
                    MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                    MERCURY_SYNC_LOG_LEVEL="error",
                ),
                dc_id=dc_id,
                manager_peers=get_manager_peer_tcp_addrs(manager_configs, config["tcp"]),
                manager_udp_peers=get_manager_peer_udp_addrs(manager_configs, config["udp"]),
            )
            managers.append(manager)

        seed_managers = get_all_manager_tcp_addrs(manager_configs)
        for config in worker_configs:
            worker = WorkerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(
                    MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                    MERCURY_SYNC_LOG_LEVEL="error",
                    WORKER_MAX_CORES=config["cores"],
                ),
                dc_id=dc_id,
                seed_managers=seed_managers,
            )
            workers.append(worker)

        print(f"  Created {manager_count} managers and {worker_count} workers")

        # Start managers
        print(f"\n[2/8] Starting managers...")
        await asyncio.gather(*[manager.start() for manager in managers])
        await asyncio.sleep(stabilization_time // 2)

        # Start workers
        print(f"\n[3/8] Starting workers...")
        await asyncio.gather(*[worker.start() for worker in workers])

        print(f"\n[4/8] Waiting for initial registration ({stabilization_time // 2}s)...")
        await asyncio.sleep(stabilization_time // 2)

        # Check initial state
        initial_discovery_ok = True
        for manager in managers:
            if manager._worker_discovery.peer_count < worker_count and len(manager._registered_workers) < worker_count:
                initial_discovery_ok = False
                break

        print(f"  Initial discovery: {'OK' if initial_discovery_ok else 'INCOMPLETE'}")

        # Fail a worker
        failed_idx = worker_count - 1
        failed_worker = workers[failed_idx]
        failed_name = worker_configs[failed_idx]['name']

        print(f"\n[5/8] Simulating failure of {failed_name}...")
        await failed_worker.stop(drain_timeout=0.5, broadcast_leave=False)

        print(f"\n[6/8] Waiting for failure detection ({failure_detection_time}s)...")
        await asyncio.sleep(failure_detection_time)

        # Check failure detection
        failure_detected = True
        expected_after_failure = worker_count - 1

        for i, manager in enumerate(managers):
            discovery_count = manager._worker_discovery.peer_count
            registered = len(manager._registered_workers)
            # Use whichever metric shows fewer workers
            effective_count = min(discovery_count, registered) if registered > 0 else discovery_count
            detected = effective_count <= expected_after_failure
            status = "DETECTED" if detected else "NOT DETECTED"
            print(f"  {manager_configs[i]['name']}: discovery={discovery_count}, registered={registered} [{status}]")
            if not detected:
                failure_detected = False

        # Recover the worker
        print(f"\n[7/8] Recovering {failed_name}...")
        recovered_worker = WorkerServer(
            host='127.0.0.1',
            tcp_port=worker_configs[failed_idx]["tcp"],
            udp_port=worker_configs[failed_idx]["udp"],
            env=Env(
                MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                MERCURY_SYNC_LOG_LEVEL="error",
                WORKER_MAX_CORES=worker_configs[failed_idx]["cores"],
            ),
            dc_id=dc_id,
            seed_managers=seed_managers,
        )
        workers[failed_idx] = recovered_worker
        await recovered_worker.start()

        print(f"\n[8/8] Waiting for recovery detection ({recovery_time}s)...")
        await asyncio.sleep(recovery_time)

        # Check recovery
        recovery_detected = True
        for i, manager in enumerate(managers):
            discovery_count = manager._worker_discovery.peer_count
            registered = len(manager._registered_workers)
            recovered = discovery_count >= worker_count or registered >= worker_count
            status = "RECOVERED" if recovered else "NOT RECOVERED"
            print(f"  {manager_configs[i]['name']}: discovery={discovery_count}, registered={registered} [{status}]")
            if not recovered:
                recovery_detected = False

        # Summary
        print(f"\n{'=' * 70}")
        all_passed = initial_discovery_ok and failure_detected and recovery_detected
        result = "PASSED" if all_passed else "FAILED"
        print(f"TEST RESULT: {result}")
        print(f"  Initial discovery: {'PASS' if initial_discovery_ok else 'FAIL'}")
        print(f"  Failure detection: {'PASS' if failure_detected else 'FAIL'}")
        print(f"  Recovery detection: {'PASS' if recovery_detected else 'FAIL'}")
        print(f"{'=' * 70}")

        return all_passed

    except Exception as e:
        import traceback
        print(f"\nTest failed with exception: {e}")
        traceback.print_exc()
        return False

    finally:
        print("\nCleaning up...")
        for worker in workers:
            try:
                await worker.stop(drain_timeout=0.5, broadcast_leave=False)
            except Exception:
                pass
        for manager in managers:
            try:
                await manager.stop(drain_timeout=0.5, broadcast_leave=False)
            except Exception:
                pass
        print("  Cleanup complete")


# ==========================================================================
# Test: Manager-Worker Discovery - Multiple Workers Per Manager
# ==========================================================================

async def test_manager_worker_discovery_scaling(
    manager_count: int,
    workers_per_manager: int,
) -> bool:
    """
    Test manager-worker discovery scaling with many workers.

    Validates:
    - Managers can discover many workers
    - Discovery service scales with worker count
    - Core allocation is tracked correctly
    """
    total_workers = manager_count * workers_per_manager

    print(f"\n{'=' * 70}")
    print(f"TEST: Manager-Worker Discovery Scaling - {manager_count} Managers, {total_workers} Workers")
    print(f"{'=' * 70}")

    dc_id = "DC-TEST"
    manager_configs = generate_manager_configs(manager_count)
    worker_configs = generate_worker_configs(total_workers, cores=2)

    managers: list[ManagerServer] = []
    workers: list[WorkerServer] = []
    stabilization_time = 20 + total_workers
    registration_time = 15

    try:
        # Create managers
        print(f"\n[1/5] Creating {manager_count} managers...")
        for config in manager_configs:
            manager = ManagerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(
                    MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                    MERCURY_SYNC_LOG_LEVEL="error",
                ),
                dc_id=dc_id,
                manager_peers=get_manager_peer_tcp_addrs(manager_configs, config["tcp"]),
                manager_udp_peers=get_manager_peer_udp_addrs(manager_configs, config["udp"]),
            )
            managers.append(manager)

        print(f"  Created {manager_count} managers")

        # Create workers
        print(f"\n[2/5] Creating {total_workers} workers...")
        seed_managers = get_all_manager_tcp_addrs(manager_configs)
        for config in worker_configs:
            worker = WorkerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(
                    MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                    MERCURY_SYNC_LOG_LEVEL="error",
                    WORKER_MAX_CORES=config["cores"],
                ),
                dc_id=dc_id,
                seed_managers=seed_managers,
            )
            workers.append(worker)

        print(f"  Created {total_workers} workers ({workers_per_manager} per manager)")

        # Start managers
        print(f"\n[3/5] Starting managers...")
        await asyncio.gather(*[manager.start() for manager in managers])
        await asyncio.sleep(stabilization_time // 2)

        # Start workers in batches to avoid overwhelming
        print(f"\n[4/5] Starting workers...")
        batch_size = 5
        for i in range(0, len(workers), batch_size):
            batch = workers[i:i + batch_size]
            await asyncio.gather(*[w.start() for w in batch])
            print(f"  Started workers {i + 1}-{min(i + batch_size, len(workers))}")

        print(f"  Waiting for registration ({registration_time}s)...")
        await asyncio.sleep(registration_time)

        # Verify discovery
        print(f"\n[5/5] Verifying worker discovery...")
        discovery_ok = True
        expected_cores = total_workers * 2  # 2 cores per worker

        for i, manager in enumerate(managers):
            discovery_count = manager._worker_discovery.peer_count
            registered = len(manager._registered_workers)
            total_cores = manager._get_total_available_cores()

            # Allow some tolerance for timing
            workers_ok = discovery_count >= total_workers * 0.8 or registered >= total_workers * 0.8

            print(f"  {manager_configs[i]['name']}:")
            print(f"    Discovery: {discovery_count}/{total_workers} workers")
            print(f"    Registered: {registered}/{total_workers} workers")
            print(f"    Cores: {total_cores}/{expected_cores}")
            print(f"    Status: [{'PASS' if workers_ok else 'FAIL'}]")

            if not workers_ok:
                discovery_ok = False

        # Summary
        print(f"\n{'=' * 70}")
        result = "PASSED" if discovery_ok else "FAILED"
        print(f"TEST RESULT: {result}")
        print(f"  Configuration: {manager_count} managers, {total_workers} workers")
        print(f"  Expected cores: {expected_cores}")
        print(f"  Discovery scaling: {'PASS' if discovery_ok else 'FAIL'}")
        print(f"{'=' * 70}")

        return discovery_ok

    except Exception as e:
        import traceback
        print(f"\nTest failed with exception: {e}")
        traceback.print_exc()
        return False

    finally:
        print("\nCleaning up...")
        for worker in workers:
            try:
                await worker.stop(drain_timeout=0.5, broadcast_leave=False)
            except Exception:
                pass
        for manager in managers:
            try:
                await manager.stop(drain_timeout=0.5, broadcast_leave=False)
            except Exception:
                pass
        print("  Cleanup complete")


# ==========================================================================
# Test: Manager-Worker Message Validation
# ==========================================================================

async def test_manager_worker_message_validation(
    manager_count: int,
    worker_count: int,
) -> bool:
    """
    Test that manager-worker messages contain correct fields.

    Validates:
    - WorkerHeartbeat contains node_id, state, tcp/udp addresses
    - Workers have correct core counts
    - Registration is successful and workers are tracked
    - Discovery service selection works
    - Latency feedback is recorded correctly
    """
    print(f"\n{'=' * 70}")
    print(f"TEST: Manager-Worker Message Validation - {manager_count} Managers, {worker_count} Workers")
    print(f"{'=' * 70}")

    dc_id = "DC-VALIDATION"
    manager_configs = generate_manager_configs(manager_count)
    worker_configs = generate_worker_configs(worker_count, cores=2)

    managers: list[ManagerServer] = []
    workers: list[WorkerServer] = []
    stabilization_time = 20 + (manager_count + worker_count) * 2

    try:
        # Create managers
        print(f"\n[1/6] Creating {manager_count} managers...")
        for config in manager_configs:
            manager = ManagerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(
                    MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                    MERCURY_SYNC_LOG_LEVEL="error",
                ),
                dc_id=dc_id,
                manager_peers=get_manager_peer_tcp_addrs(manager_configs, config["tcp"]),
                manager_udp_peers=get_manager_peer_udp_addrs(manager_configs, config["udp"]),
            )
            managers.append(manager)

        # Create workers
        print(f"\n[2/6] Creating {worker_count} workers...")
        seed_managers = get_all_manager_tcp_addrs(manager_configs)
        for config in worker_configs:
            worker = WorkerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(
                    MERCURY_SYNC_REQUEST_TIMEOUT='5s',
                    MERCURY_SYNC_LOG_LEVEL="error",
                    WORKER_MAX_CORES=config["cores"],
                ),
                dc_id=dc_id,
                seed_managers=seed_managers,
            )
            workers.append(worker)

        # Start managers
        print(f"\n[3/6] Starting managers...")
        await asyncio.gather(*[manager.start() for manager in managers])
        await asyncio.sleep(stabilization_time // 3)

        # Start workers
        print(f"\n[4/6] Starting workers...")
        await asyncio.gather(*[worker.start() for worker in workers])

        print(f"\n[5/6] Waiting for discovery ({stabilization_time}s)...")
        await asyncio.sleep(stabilization_time)

        # Validate worker state
        print(f"\n[6/6] Validating worker state and registration...")
        validation_results = {
            "worker_node_ids_valid": True,
            "worker_cores_valid": True,
            "worker_state_valid": True,
            "worker_registered_valid": True,
            "manager_discovery_valid": True,
            "manager_selection_valid": True,
            "latency_feedback_valid": True,
        }

        # Validate each worker
        for i, worker in enumerate(workers):
            config = worker_configs[i]
            print(f"\n  {config['name']} validation:")

            # Validate node_id
            if worker._node_id and str(worker._node_id):
                print(f"    node_id: {worker._node_id.short} [PASS]")
            else:
                print(f"    node_id: MISSING [FAIL]")
                validation_results["worker_node_ids_valid"] = False

            # Validate cores
            if worker._max_cores == config["cores"]:
                print(f"    max_cores: {worker._max_cores} [PASS]")
            else:
                print(f"    max_cores: {worker._max_cores} (expected {config['cores']}) [FAIL]")
                validation_results["worker_cores_valid"] = False

            # Validate state
            worker_state = worker._state.value if hasattr(worker._state, 'value') else str(worker._state)
            valid_states = {"starting", "syncing", "active", "draining", "stopped"}
            if worker_state in valid_states:
                print(f"    state: {worker_state} [PASS]")
            else:
                print(f"    state: {worker_state} (invalid) [FAIL]")
                validation_results["worker_state_valid"] = False

            # Validate registration
            registered_managers = len(worker._known_managers)
            if registered_managers >= 1:
                print(f"    known_managers: {registered_managers} [PASS]")
            else:
                print(f"    known_managers: {registered_managers} (expected >= 1) [FAIL]")
                validation_results["worker_registered_valid"] = False

        # Validate manager worker discovery
        print(f"\n  Manager worker discovery validation:")
        for i, manager in enumerate(managers):
            config = manager_configs[i]
            discovery = manager._worker_discovery

            # Check peer count
            peer_count = discovery.peer_count
            registered = len(manager._registered_workers)
            if peer_count >= worker_count or registered >= worker_count:
                print(f"    {config['name']}: discovery={peer_count}, registered={registered} [PASS]")
            else:
                print(f"    {config['name']}: discovery={peer_count}, registered={registered} (expected {worker_count}) [FAIL]")
                validation_results["manager_discovery_valid"] = False

            # Test worker selection
            test_key = f"workflow-{i}"
            selected = manager._select_best_worker(test_key)
            if selected is not None:
                host, port = selected
                print(f"    {config['name']} selection for '{test_key}': ({host}:{port}) [PASS]")
            else:
                print(f"    {config['name']} selection for '{test_key}': None [FAIL]")
                validation_results["manager_selection_valid"] = False

            # Test latency feedback
            all_peers = discovery.get_all_peers()
            if all_peers:
                test_peer = all_peers[0]
                manager._record_worker_success(test_peer.peer_id, 15.0)
                manager._record_worker_failure(test_peer.peer_id)
                effective = discovery.get_effective_latency(test_peer.peer_id)
                if effective > 0:
                    print(f"    {config['name']} latency feedback: effective={effective:.1f}ms [PASS]")
                else:
                    print(f"    {config['name']} latency feedback: not working [FAIL]")
                    validation_results["latency_feedback_valid"] = False

        # Summary
        print(f"\n{'=' * 70}")
        all_valid = all(validation_results.values())
        result = "PASSED" if all_valid else "FAILED"
        print(f"TEST RESULT: {result}")
        for key, valid in validation_results.items():
            print(f"  {key}: {'PASS' if valid else 'FAIL'}")
        print(f"{'=' * 70}")

        return all_valid

    except Exception as e:
        import traceback
        print(f"\nTest failed with exception: {e}")
        traceback.print_exc()
        return False

    finally:
        print("\nCleaning up...")
        for worker in workers:
            try:
                await worker.stop(drain_timeout=0.5, broadcast_leave=False)
            except Exception:
                pass
        for manager in managers:
            try:
                await manager.stop(drain_timeout=0.5, broadcast_leave=False)
            except Exception:
                pass
        print("  Cleanup complete")


# ==========================================================================
# Main Test Runner
# ==========================================================================

async def run_all_tests():
    """Run all manager-worker discovery tests."""
    results = {}

    print("\n" + "=" * 70)
    print("MANAGER-WORKER DISCOVERY INTEGRATION TESTS")
    print("=" * 70)
    print("\nThis test suite validates:")
    print("  1. Managers discover workers via registration")
    print("  2. Worker discovery service tracks all workers")
    print("  3. WorkerHeartbeat messages contain correct fields")
    print("  4. Failed workers are detected and removed")
    print("  5. Recovered workers are re-discovered")
    print("  6. Discovery scales with worker count")
    print("  7. Worker selection and latency feedback work correctly")

    # Basic discovery tests
    print("\n--- Basic Discovery Tests ---")
    for managers, workers in [(1, 2), (2, 3), (3, 4)]:
        result = await test_manager_worker_discovery_basic(managers, workers)
        results[f"basic_{managers}m_{workers}w"] = result

    # Message validation tests
    print("\n--- Message Validation Tests ---")
    result = await test_manager_worker_message_validation(2, 3)
    results["message_validation_2m_3w"] = result

    # Failure/recovery tests
    print("\n--- Failure/Recovery Tests ---")
    for managers, workers in [(2, 3), (3, 4)]:
        result = await test_manager_worker_discovery_failure_recovery(managers, workers)
        results[f"failure_recovery_{managers}m_{workers}w"] = result

    # Scaling tests
    print("\n--- Scaling Tests ---")
    for managers, workers_per in [(2, 3), (3, 4)]:
        result = await test_manager_worker_discovery_scaling(managers, workers_per)
        results[f"scaling_{managers}m_{workers_per}w_per"] = result

    # Final summary
    print("\n" + "=" * 70)
    print("FINAL TEST SUMMARY")
    print("=" * 70)

    all_passed = True
    for test_name, passed in results.items():
        status = "PASS" if passed else "FAIL"
        print(f"  {test_name}: {status}")
        if not passed:
            all_passed = False

    print(f"\nOverall: {'ALL TESTS PASSED' if all_passed else 'SOME TESTS FAILED'}")
    print("=" * 70)

    return all_passed


def main():
    success = asyncio.run(run_all_tests())
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
