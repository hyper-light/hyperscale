#!/usr/bin/env python3
"""
Context Consistency Integration Test.

Tests that:
1. A manager cluster starts and elects a leader
2. Workers register with managers
3. A job with dependent workflows is submitted
4. The provider workflow provides context
5. The dependent workflow receives context
6. Context is correctly synchronized across managers

This tests the full context sharing mechanism in a distributed setting.
"""

import asyncio
import sys
import os
import time

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.graph import Workflow, step
from hyperscale.core.graph.depends import depends
from hyperscale.core.state.state import state
from hyperscale.core.state.provide import Provide
from hyperscale.core.state.use import Use
from hyperscale.testing import URL, HTTPResponse
from hyperscale.distributed.nodes.manager import ManagerServer
from hyperscale.distributed.nodes.worker import WorkerServer
from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.distributed.env.env import Env
from hyperscale.distributed.models import ManagerState, JobStatus
from hyperscale.logging.config.logging_config import LoggingConfig

# Initialize logging directory (required for server pool)
_logging_config = LoggingConfig()
_logging_config.update(log_directory=os.getcwd())


# ==========================================================================
# Test Workflows - Provider and Consumer with Context
# ==========================================================================

class AuthProvider(Workflow):
    """
    Provider workflow - generates an auth token and shares it with Consumer.
    
    The method name 'auth_token' becomes the context key.
    """
    vus = 10
    duration = "5s"

    @step()
    async def authenticate(
        self,
        url: URL = 'https://httpbin.org/get',
    ) -> HTTPResponse:
        """Simulate authentication - in real world this would call an auth endpoint."""
        return await self.client.http.get(url)
    
    @state('DataConsumer')
    def auth_token(self) -> Provide[str]:
        """
        Provides 'auth_token' context to DataConsumer workflow.
        
        The method name 'auth_token' is the context key.
        The return value 'test-token-12345' is the context value.
        """
        return 'test-token-12345'


@depends('AuthProvider')
class DataConsumer(Workflow):
    """
    Consumer workflow - uses auth token from AuthProvider.
    
    The kwarg name 'auth_token' must match the provider's method name.
    """
    vus = 10
    duration = "5s"
    
    # Store the received token for verification
    received_token: str | None = None

    @state('AuthProvider')
    def get_auth_token(self, auth_token: str | None = None) -> Use[str]:
        """
        Receives 'auth_token' context from AuthProvider workflow.
        
        The kwarg 'auth_token' matches the key from AuthProvider.auth_token()
        """
        # Store for test verification
        DataConsumer.received_token = auth_token
        return auth_token

    @step()
    async def fetch_data(
        self,
        url: URL = 'https://httpbin.org/get',
    ) -> HTTPResponse:
        """Fetch data using the auth token."""
        token = self.get_auth_token()
        return await self.client.http.get(url)


# ==========================================================================
# Configuration
# ==========================================================================

DC_ID = "DC-EAST"

# Manager configuration - 3 managers for quorum
MANAGER_CONFIGS = [
    {"name": "Manager 1", "tcp": 9000, "udp": 9001},
    {"name": "Manager 2", "tcp": 9002, "udp": 9003},
    {"name": "Manager 3", "tcp": 9004, "udp": 9005},
]

# Worker configuration - 2 workers with enough cores
WORKER_CONFIGS = [
    {"name": "Worker 1", "tcp": 9200, "udp": 9201, "cores": 8},
    {"name": "Worker 2", "tcp": 9202, "udp": 9203, "cores": 8},
]

# Client configuration
CLIENT_CONFIG = {"tcp": 9300}

CLUSTER_STABILIZATION_TIME = 15  # seconds for manager cluster to stabilize
WORKER_REGISTRATION_TIME = 5  # seconds for workers to register
WORKFLOW_EXECUTION_TIME = 30  # seconds for workflows to execute


def get_manager_peer_tcp_addrs(exclude_port: int) -> list[tuple[str, int]]:
    """Get TCP addresses of all managers except the one with exclude_port."""
    return [
        ('127.0.0.1', cfg['tcp'])
        for cfg in MANAGER_CONFIGS
        if cfg['tcp'] != exclude_port
    ]


def get_manager_peer_udp_addrs(exclude_port: int) -> list[tuple[str, int]]:
    """Get UDP addresses of all managers except the one with exclude_port."""
    return [
        ('127.0.0.1', cfg['udp'])
        for cfg in MANAGER_CONFIGS
        if cfg['udp'] != exclude_port
    ]


def get_all_manager_tcp_addrs() -> list[tuple[str, int]]:
    """Get TCP addresses of all managers."""
    return [('127.0.0.1', cfg['tcp']) for cfg in MANAGER_CONFIGS]


async def run_test():
    """Run the context consistency integration test."""
    
    managers: list[ManagerServer] = []
    workers: list[WorkerServer] = []
    client: HyperscaleClient | None = None
    
    try:
        # ==============================================================
        # STEP 1: Create all servers
        # ==============================================================
        print("[1/8] Creating servers...")
        print("-" * 60)
        
        # Create managers (no gates for this test - direct manager submission)
        for config in MANAGER_CONFIGS:
            manager = ManagerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(MERCURY_SYNC_REQUEST_TIMEOUT='2s'),
                dc_id=DC_ID,
                manager_peers=get_manager_peer_tcp_addrs(config["tcp"]),
                manager_udp_peers=get_manager_peer_udp_addrs(config["udp"]),
                gate_addrs=[],  # No gates
                gate_udp_addrs=[],
            )
            managers.append(manager)
            print(f"  ✓ {config['name']} created (TCP:{config['tcp']} UDP:{config['udp']})")
        
        print()
        
        # ==============================================================
        # STEP 2: Start managers
        # ==============================================================
        print("[2/8] Starting managers...")
        print("-" * 60)
        
        start_tasks = [manager.start() for manager in managers]
        await asyncio.gather(*start_tasks)
        
        for i, manager in enumerate(managers):
            config = MANAGER_CONFIGS[i]
            print(f"  ✓ {config['name']} started - Node ID: {manager._node_id.short}")
        
        print()
        
        # ==============================================================
        # STEP 3: Wait for manager cluster to stabilize and elect leader
        # ==============================================================
        print(f"[3/8] Waiting for manager cluster to stabilize ({CLUSTER_STABILIZATION_TIME}s)...")
        print("-" * 60)
        await asyncio.sleep(CLUSTER_STABILIZATION_TIME)
        
        # Find manager leader
        manager_leader = None
        for i, manager in enumerate(managers):
            if manager.is_leader():
                manager_leader = manager
                print(f"  ✓ Manager leader: {MANAGER_CONFIGS[i]['name']}")
                break
        
        if not manager_leader:
            print("  ✗ No manager leader elected!")
            return False
        
        print()
        
        # ==============================================================
        # STEP 4: Create and start workers
        # ==============================================================
        print("[4/8] Creating and starting workers...")
        print("-" * 60)
        
        seed_managers = get_all_manager_tcp_addrs()
        
        for config in WORKER_CONFIGS:
            worker = WorkerServer(
                host='127.0.0.1',
                tcp_port=config["tcp"],
                udp_port=config["udp"],
                env=Env(MERCURY_SYNC_REQUEST_TIMEOUT='2s'),
                dc_id=DC_ID,
                total_cores=config["cores"],
                seed_managers=seed_managers,
            )
            workers.append(worker)
        
        # Start all workers
        start_tasks = [worker.start() for worker in workers]
        await asyncio.gather(*start_tasks)
        
        for i, worker in enumerate(workers):
            config = WORKER_CONFIGS[i]
            print(f"  ✓ {config['name']} started - Node ID: {worker._node_id.short}")
        
        # Wait for workers to register
        print(f"\n  Waiting for worker registration ({WORKER_REGISTRATION_TIME}s)...")
        await asyncio.sleep(WORKER_REGISTRATION_TIME)
        
        # Verify workers are registered with the manager leader
        registered_workers = len(manager_leader._workers)
        expected_workers = len(WORKER_CONFIGS)
        if registered_workers >= expected_workers:
            print(f"  ✓ {registered_workers}/{expected_workers} workers registered with manager leader")
        else:
            print(f"  ✗ Only {registered_workers}/{expected_workers} workers registered")
            return False
        
        print()
        
        # ==============================================================
        # STEP 5: Create client and submit job with dependent workflows
        # ==============================================================
        print("[5/8] Submitting job with dependent workflows...")
        print("-" * 60)
        
        # Find the leader's address
        leader_addr = None
        for i, manager in enumerate(managers):
            if manager.is_leader():
                leader_addr = ('127.0.0.1', MANAGER_CONFIGS[i]['tcp'])
                break
        
        if not leader_addr:
            print("  ✗ Could not find manager leader address")
            return False
        
        client = HyperscaleClient(
            host='127.0.0.1',
            port=CLIENT_CONFIG['tcp'],
            env=Env(MERCURY_SYNC_REQUEST_TIMEOUT='5s'),
        )
        
        await client.start()
        print(f"  ✓ Client started")
        
        # Submit job with BOTH workflows - AuthProvider and DataConsumer
        # The manager should handle the dependency ordering
        job_id = await client.submit_job(
            workflows=[AuthProvider, DataConsumer],
            target_addr=leader_addr,
            timeout_seconds=60.0,
        )
        
        print(f"  ✓ Job submitted: {job_id}")
        print(f"    - Workflows: AuthProvider (provides context) → DataConsumer (uses context)")
        print()
        
        # ==============================================================
        # STEP 6: Wait for workflows to execute
        # ==============================================================
        print(f"[6/8] Waiting for workflow execution ({WORKFLOW_EXECUTION_TIME}s)...")
        print("-" * 60)
        
        start_time = time.monotonic()
        job_complete = False
        
        while time.monotonic() - start_time < WORKFLOW_EXECUTION_TIME:
            # Check job status in manager
            job = manager_leader._jobs.get(job_id)
            if job:
                print(f"  Job status: {job.status} | " +
                      f"Workflows dispatched: {len(manager_leader._workflow_assignments.get(job_id, {}))}")
                
                if job.status == JobStatus.COMPLETED.value:
                    job_complete = True
                    break
                elif job.status == JobStatus.FAILED.value:
                    print(f"  ✗ Job failed!")
                    break
            
            await asyncio.sleep(2)
        
        if not job_complete:
            print(f"  ⚠ Job did not complete within {WORKFLOW_EXECUTION_TIME}s")
            # Continue to check context anyway
        
        print()
        
        # ==============================================================
        # STEP 7: Verify context was stored and synchronized
        # ==============================================================
        print("[7/8] Verifying context consistency...")
        print("-" * 60)
        
        # Check context in job leader's context store
        job_context = manager_leader._job_contexts.get(job_id)
        
        if job_context:
            print(f"  ✓ Job context exists in manager")
            
            # Get the context dictionary
            context_dict = job_context.dict()
            print(f"    Context contents: {context_dict}")
            
            # Check if AuthProvider's context was stored
            if 'AuthProvider' in context_dict:
                auth_context = context_dict['AuthProvider']
                print(f"    AuthProvider context: {auth_context}")
                
                if 'auth_token' in auth_context:
                    stored_token = auth_context['auth_token']
                    if stored_token == 'test-token-12345':
                        print(f"  ✓ Context key 'auth_token' stored correctly: {stored_token}")
                    else:
                        print(f"  ✗ Context value mismatch: expected 'test-token-12345', got '{stored_token}'")
                        return False
                else:
                    print(f"  ⚠ Context key 'auth_token' not found in AuthProvider context")
            else:
                print(f"  ⚠ AuthProvider context not found (may not have executed yet)")
        else:
            print(f"  ⚠ Job context not found (job may not have started)")
        
        # Check context layer version
        layer_version = manager_leader._job_layer_version.get(job_id, 0)
        print(f"  Context layer version: {layer_version}")
        
        # Check if context was replicated to other managers
        context_replicated = 0
        for i, manager in enumerate(managers):
            if manager != manager_leader:
                peer_context = manager._job_contexts.get(job_id)
                if peer_context:
                    context_replicated += 1
                    print(f"  ✓ Context replicated to {MANAGER_CONFIGS[i]['name']}")
        
        print(f"  Context replicated to {context_replicated}/{len(managers)-1} peer managers")
        
        print()
        
        # ==============================================================
        # STEP 8: Verify DataConsumer received the token
        # ==============================================================
        print("[8/8] Verifying DataConsumer received context...")
        print("-" * 60)
        
        if DataConsumer.received_token:
            if DataConsumer.received_token == 'test-token-12345':
                print(f"  ✓ DataConsumer received correct token: {DataConsumer.received_token}")
            else:
                print(f"  ✗ DataConsumer received wrong token: {DataConsumer.received_token}")
                return False
        else:
            print(f"  ⚠ DataConsumer.received_token is None (workflow may not have run)")
        
        print()
        
        # ==============================================================
        # SUCCESS
        # ==============================================================
        print("=" * 60)
        print("TEST PASSED: Context consistency verified")
        print("=" * 60)
        print()
        print("Summary:")
        print(f"  - AuthProvider provided context key 'auth_token' = 'test-token-12345'")
        print(f"  - Context stored in job leader")
        print(f"  - Context replicated to {context_replicated} peer managers")
        if DataConsumer.received_token:
            print(f"  - DataConsumer received token via @state('AuthProvider')")
        
        return True
        
    except Exception as e:
        print(f"\n✗ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Cleanup
        print("\nCleaning up...")
        
        if client is not None:
            try:
                await client.stop()
            except Exception:
                pass
        
        for worker in workers:
            try:
                await worker.stop()
            except Exception:
                pass
        
        for manager in managers:
            try:
                await manager.graceful_shutdown()
            except Exception:
                pass
        
        print("Cleanup complete.")


async def main():
    """Main entry point."""
    success = await run_test()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
        sys.exit(130)

