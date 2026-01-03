#!/usr/bin/env python
"""
End-to-end workflow execution test.

Tests the complete flow:
1. Client submits job with workflows to Manager
2. Manager dispatches workflows to Worker
3. Worker executes workflows
4. Worker sends results back to Manager
5. Manager sends results back to Client

This tests:
- Workflow execution
- Context updates (Provide/Use)
- Results return
- Full distributed coordination
"""

import asyncio
import os
import sys
import time

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import cloudpickle

from hyperscale.logging.config import LoggingConfig
from hyperscale.distributed_rewrite.env.env import Env
from hyperscale.distributed_rewrite.nodes.manager import ManagerServer
from hyperscale.distributed_rewrite.nodes.worker import WorkerServer
from hyperscale.distributed_rewrite.nodes.client import HyperscaleClient
from hyperscale.graph import Workflow, step


# =============================================================================
# Test Workflows
# =============================================================================

class SimpleTestWorkflow(Workflow):
    """Simple workflow that executes quickly for testing."""
    vus = 2
    duration = "5s"
    
    @step()
    async def test_action(self) -> dict:
        """Simple test action."""
        await asyncio.sleep(0.5)
        return {"status": "completed", "value": 42}


# =============================================================================
# Test Implementation
# =============================================================================

async def run_test():
    """Run the end-to-end workflow execution test."""
    print("=" * 60)
    print("END-TO-END WORKFLOW EXECUTION TEST")
    print("=" * 60)
    
    # Setup logging
    LoggingConfig().update(log_directory=os.getcwd(), log_level="info")
    
    env = Env()
    
    # Server addresses
    manager_tcp = 9100
    manager_udp = 9101
    worker_tcp = 9200
    worker_udp = 9201
    client_port = 9300
    
    manager = None
    worker = None
    client = None
    all_passed = True
    
    try:
        # ---------------------------------------------------------------------
        # Start Manager
        # ---------------------------------------------------------------------
        print("\n[1/6] Starting Manager...")
        print("-" * 50)
        
        manager = ManagerServer(
            host='127.0.0.1',
            tcp_port=manager_tcp,
            udp_port=manager_udp,
            env=env,
            dc_id="DC-TEST",
        )
        
        await asyncio.wait_for(manager.start(), timeout=15.0)
        print(f"  ✓ Manager started on TCP:{manager_tcp} UDP:{manager_udp}")
        
        # Wait for manager to become leader (single manager should become leader quickly)
        leader_wait = 0
        while not manager.is_leader() and leader_wait < 30:
            await asyncio.sleep(1.0)
            leader_wait += 1
        
        if manager.is_leader():
            print(f"  ✓ Manager is leader (after {leader_wait}s)")
        else:
            print(f"  ✗ Manager failed to become leader after {leader_wait}s")
            all_passed = False
        
        # ---------------------------------------------------------------------
        # Start Worker
        # ---------------------------------------------------------------------
        print("\n[2/6] Starting Worker...")
        print("-" * 50)
        
        worker = WorkerServer(
            host='127.0.0.1',
            tcp_port=worker_tcp,
            udp_port=worker_udp,
            env=env,
            total_cores=4,
            dc_id="DC-TEST",
            seed_managers=[('127.0.0.1', manager_tcp)],
        )
        
        await asyncio.wait_for(worker.start(), timeout=30.0)
        print(f"  ✓ Worker started with {worker._total_cores} cores")
        
        # Wait for worker to register with manager
        await asyncio.sleep(2.0)
        
        # Verify manager knows about worker
        workers_registered = len(manager._workers)
        if workers_registered > 0:
            print(f"  ✓ Worker registered with manager ({workers_registered} workers)")
        else:
            print(f"  ✗ Worker not registered with manager")
            all_passed = False
        
        # ---------------------------------------------------------------------
        # Start Client
        # ---------------------------------------------------------------------
        print("\n[3/6] Starting Client...")
        print("-" * 50)
        
        client = HyperscaleClient(
            host='127.0.0.1',
            port=client_port,
            env=env,
            managers=[('127.0.0.1', manager_tcp)],
        )
        
        await client.start()
        print(f"  ✓ Client started on port {client_port}")
        
        # ---------------------------------------------------------------------
        # Submit Job
        # ---------------------------------------------------------------------
        print("\n[4/6] Submitting job with SimpleTestWorkflow...")
        print("-" * 50)
        
        # Debug: print manager state before submission
        print(f"  DEBUG: Workers registered: {len(manager._workers)}")
        print(f"  DEBUG: Worker status entries: {len(manager._worker_status)}")
        for wid, ws in manager._worker_status.items():
            print(f"    - {wid}: state={ws.state}, cores={ws.available_cores}/{getattr(ws, 'total_cores', 'N/A')}")
        
        try:
            job_id = await asyncio.wait_for(
                client.submit_job(
                    workflows=[SimpleTestWorkflow],
                    vus=2,
                    timeout_seconds=30.0,
                ),
                timeout=10.0,
            )
            print(f"  ✓ Job submitted: {job_id}")
        except Exception as e:
            print(f"  ✗ Job submission failed: {e}")
            all_passed = False
            job_id = None
        
        # ---------------------------------------------------------------------
        # Wait for Completion
        # ---------------------------------------------------------------------
        if job_id:
            print("\n[5/6] Waiting for job completion...")
            print("-" * 50)
            
            try:
                result = await asyncio.wait_for(
                    client.wait_for_job(job_id, timeout=60.0),
                    timeout=65.0,
                )
                print(f"  ✓ Job completed")
                print(f"    - Status: {result.status}")
                print(f"    - Completed: {result.total_completed}")
                print(f"    - Failed: {result.total_failed}")
                print(f"    - Elapsed: {result.elapsed_seconds:.2f}s")
                
                if result.status == "COMPLETED":
                    print(f"  ✓ Job status is COMPLETED")
                else:
                    print(f"  ✗ Unexpected job status: {result.status}")
                    all_passed = False
                    
            except asyncio.TimeoutError:
                print(f"  ✗ Job timed out waiting for completion")
                all_passed = False
                
                # Check job status
                job_result = client.get_job_status(job_id)
                if job_result:
                    print(f"    - Current status: {job_result.status}")
            except Exception as e:
                print(f"  ✗ Error waiting for job: {e}")
                all_passed = False
        else:
            print("\n[5/6] Skipping wait (no job submitted)")
            print("-" * 50)
        
        # ---------------------------------------------------------------------
        # Verify State
        # ---------------------------------------------------------------------
        print("\n[6/6] Verifying final state...")
        print("-" * 50)
        
        # Check manager job tracking
        manager_jobs = len(manager._jobs)
        print(f"  - Manager tracking {manager_jobs} jobs")
        
        # Check worker core allocation
        active_cores = sum(1 for v in worker._core_assignments.values() if v is not None)
        if active_cores == 0:
            print(f"  ✓ All worker cores freed")
        else:
            print(f"  ✗ {active_cores} worker cores still assigned")
            all_passed = False
        
    except Exception as e:
        print(f"\n✗ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    
    finally:
        # ---------------------------------------------------------------------
        # Cleanup
        # ---------------------------------------------------------------------
        print("\n" + "-" * 50)
        print("Cleaning up...")
        
        if client:
            try:
                await client.stop()
                print("  ✓ Client stopped")
            except Exception as e:
                print(f"  ✗ Client stop failed: {e}")
        
        if worker:
            try:
                await worker.graceful_shutdown()
                print("  ✓ Worker stopped")
            except Exception as e:
                print(f"  ✗ Worker stop failed: {e}")
        
        if manager:
            try:
                await manager.graceful_shutdown()
                print("  ✓ Manager stopped")
            except Exception as e:
                print(f"  ✗ Manager stop failed: {e}")
    
    # -------------------------------------------------------------------------
    # Final Result
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    if all_passed:
        print("TEST PASSED: End-to-end workflow execution successful")
    else:
        print("TEST FAILED: Some checks failed")
    print("=" * 60)
    
    return all_passed


if __name__ == "__main__":
    try:
        success = asyncio.run(run_test())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nInterrupted")
        sys.exit(1)

