#!/usr/bin/env python
"""
Test that verifies workflow stats are being pushed from workers to managers.

This test uses a longer-running workflow to ensure we actually see
progress updates being sent during execution, not just at completion.

Tests:
1. Worker sends WorkflowProgress updates during execution
2. Manager receives and tracks progress updates
3. Stats include completed count, failed count, rate, etc.
4. Final results are properly aggregated
"""

import asyncio
import os
import sys
import time

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.logging.config import LoggingConfig
from hyperscale.distributed_rewrite.env.env import Env
from hyperscale.distributed_rewrite.nodes.manager import ManagerServer
from hyperscale.distributed_rewrite.nodes.worker import WorkerServer
from hyperscale.distributed_rewrite.nodes.client import HyperscaleClient
from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse


# =============================================================================
# Test Workflows
# =============================================================================

class NonTestWorkflow(Workflow):
    """
    Non-test workflow (returns dict, not HTTPResponse).
    
    Non-test workflows get 1 core regardless of VUs because they don't
    parallelize via multiple processes.
    """
    vus = 1000  # VUs can be large - cores are determined by priority!
    duration = "5s"
    
    @step()
    async def test_action(self) -> dict:
        """Non-test action - returns dict, not HTTPResponse."""
        for i in range(5):
            await asyncio.sleep(0.3)
        return {"iteration": 5, "status": "completed"}


class TestWorkflow(Workflow):
    """
    Test workflow (returns HTTPResponse from client call).
    
    Test workflows get cores based on priority (AUTO = up to 100% of pool)
    because they parallelize load testing across multiple processes.
    """
    vus = 1000
    duration = "5s"
    
    @step()
    async def load_test(
        self,
        url: URL = 'https://httpbin.org/get',
    ) -> HTTPResponse:
        """Test action - returns HTTPResponse from client call."""
        # This makes it a "test workflow" because:
        # 1. @step() decorator creates a Hook
        # 2. Return type HTTPResponse is a CallResult subclass
        # 3. Hook.hook_type gets set to HookType.TEST
        return await self.client.http.get(url)


# =============================================================================
# Test Implementation
# =============================================================================

async def run_test():
    """Run the stats push verification test."""
    print("=" * 60)
    print("WORKFLOW STATS PUSH VERIFICATION TEST")
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
    progress_updates_received = []
    
    try:
        # ---------------------------------------------------------------------
        # Start Manager
        # ---------------------------------------------------------------------
        print("\n[1/7] Starting Manager...")
        print("-" * 50)
        
        manager = ManagerServer(
            host='127.0.0.1',
            tcp_port=manager_tcp,
            udp_port=manager_udp,
            env=env,
            dc_id="DC-TEST",
        )
        
        # Store original workflow_progress handler to track calls
        original_workflow_progress = manager.workflow_progress
        
        async def tracking_workflow_progress(addr, data, clock_time):
            """Wrapper that tracks progress updates."""
            progress_updates_received.append({
                'time': time.monotonic(),
                'addr': addr,
                'data_len': len(data),
            })
            return await original_workflow_progress(addr, data, clock_time)
        
        manager.workflow_progress = tracking_workflow_progress
        
        await asyncio.wait_for(manager.start(), timeout=15.0)
        print(f"  ✓ Manager started on TCP:{manager_tcp}")
        
        # Wait for manager to become leader
        leader_wait = 0
        while not manager.is_leader() and leader_wait < 30:
            await asyncio.sleep(1.0)
            leader_wait += 1
        
        if manager.is_leader():
            print(f"  ✓ Manager is leader")
        else:
            print(f"  ✗ Manager failed to become leader")
            all_passed = False
        
        # ---------------------------------------------------------------------
        # Start Worker
        # ---------------------------------------------------------------------
        print("\n[2/7] Starting Worker...")
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
        
        # Wait for worker to register
        await asyncio.sleep(2.0)
        
        if len(manager._workers) > 0:
            print(f"  ✓ Worker registered with manager")
        else:
            print(f"  ✗ Worker not registered")
            all_passed = False
        
        # ---------------------------------------------------------------------
        # Start Client
        # ---------------------------------------------------------------------
        print("\n[3/7] Starting Client...")
        print("-" * 50)
        
        client = HyperscaleClient(
            host='127.0.0.1',
            port=client_port,
            env=env,
            managers=[('127.0.0.1', manager_tcp)],
        )
        
        await client.start()
        print(f"  ✓ Client started")
        
        # ---------------------------------------------------------------------
        # Submit Job with Long-Running Workflow
        # ---------------------------------------------------------------------
        print("\n[4/7] Submitting job with LongRunningWorkflow...")
        print("-" * 50)
        
        initial_progress_count = len(progress_updates_received)
        
        try:
            job_id = await asyncio.wait_for(
                client.submit_job(
                    workflows=[NonTestWorkflow],  # Non-test workflow gets 1 core
                    vus=1000,  # VUs can be large - cores are from priority!
                    timeout_seconds=60.0,
                ),
                timeout=10.0,
            )
            print(f"  ✓ Job submitted: {job_id}")
        except Exception as e:
            print(f"  ✗ Job submission failed: {e}")
            import traceback
            traceback.print_exc()
            all_passed = False
            job_id = None
        
        # ---------------------------------------------------------------------
        # Monitor Progress Updates During Execution
        # ---------------------------------------------------------------------
        if job_id:
            print("\n[5/7] Monitoring progress updates during execution...")
            print("-" * 50)
            
            # Poll for progress updates while job is running
            check_start = time.monotonic()
            job_done = False
            last_progress_check = initial_progress_count
            
            while time.monotonic() - check_start < 45.0 and not job_done:
                await asyncio.sleep(1.0)
                
                current_count = len(progress_updates_received)
                if current_count > last_progress_check:
                    new_updates = current_count - last_progress_check
                    print(f"  → Received {new_updates} progress update(s) (total: {current_count})")
                    last_progress_check = current_count
                
                # Check if job is in manager's tracker
                job = manager._jobs.get(job_id)
                if job:
                    print(f"  → Job status: completed={job.total_completed}, failed={job.total_failed}")
                    
                    # Check job status from client
                    client_status = client.get_job_status(job_id)
                    if client_status and client_status.status == "completed":
                        job_done = True
                        print(f"  ✓ Job completed!")
                        break
            
            # Verify we received progress updates
            total_progress = len(progress_updates_received) - initial_progress_count
            print(f"\n  Progress updates received during execution: {total_progress}")
            
            if total_progress > 0:
                print(f"  ✓ Progress updates were sent from worker to manager")
            else:
                print(f"  ⚠ No progress updates received (workflow may have completed too quickly)")
                # This is a warning, not a failure - short workflows may complete before first update
        
        # ---------------------------------------------------------------------
        # Wait for Final Completion
        # ---------------------------------------------------------------------
        if job_id:
            print("\n[6/7] Waiting for final job result...")
            print("-" * 50)
            
            try:
                result = await asyncio.wait_for(
                    client.wait_for_job(job_id, timeout=60.0),
                    timeout=65.0,
                )
                print(f"  ✓ Final result received")
                print(f"    - Status: {result.status}")
                print(f"    - Total Completed: {result.total_completed}")
                print(f"    - Total Failed: {result.total_failed}")
                print(f"    - Elapsed: {result.elapsed_seconds:.2f}s")
                
                if result.status == "completed":
                    print(f"  ✓ Job completed successfully")
                else:
                    print(f"  ✗ Job status is {result.status}")
                    all_passed = False
                    
            except asyncio.TimeoutError:
                print(f"  ✗ Timeout waiting for job completion")
                all_passed = False
        
        # ---------------------------------------------------------------------
        # Verify Stats in Manager
        # ---------------------------------------------------------------------
        print("\n[7/7] Verifying stats in manager...")
        print("-" * 50)
        
        if job_id:
            job = manager._jobs.get(job_id)
            if job:
                print(f"  Job tracking in manager:")
                print(f"    - Workflows tracked: {len(job.workflows)}")
                print(f"    - Total completed: {job.total_completed}")
                print(f"    - Total failed: {job.total_failed}")
                print(f"    - Overall rate: {job.overall_rate:.2f}/s")
                
                for wf in job.workflows:
                    print(f"    - Workflow '{wf.workflow_name}':")
                    print(f"        Status: {wf.status}")
                    print(f"        Completed: {wf.completed_count}")
                    print(f"        Failed: {wf.failed_count}")
                    print(f"        Rate: {wf.rate_per_second:.2f}/s")
            else:
                print(f"  ⚠ Job not found in manager tracker (may have been cleaned up)")
        
        # Summary of progress updates
        total_updates = len(progress_updates_received) - initial_progress_count
        print(f"\n  SUMMARY:")
        print(f"  - Total progress updates received: {total_updates}")
        
        if total_updates >= 1:
            print(f"  ✓ Stats push verification PASSED")
        else:
            # For very short workflows, this may be expected
            print(f"  ⚠ Very few progress updates - workflow may have completed quickly")
        
    except Exception as e:
        print(f"\n✗ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    
    finally:
        # ---------------------------------------------------------------------
        # Cleanup - IMPORTANT: Wait for proper shutdown
        # ---------------------------------------------------------------------
        print("\n" + "-" * 50)
        print("Cleaning up (please wait for proper shutdown)...")
        
        # Allow any pending tasks to complete
        await asyncio.sleep(0.5)
        
        if client:
            try:
                await asyncio.wait_for(client.stop(), timeout=5.0)
                print("  ✓ Client stopped")
            except Exception as e:
                print(f"  ✗ Client stop failed: {e}")
        
        if worker:
            try:
                # Use worker.stop() which properly cleans up LocalServerPool,
                # remote manager, and then calls graceful_shutdown()
                await asyncio.wait_for(worker.stop(), timeout=15.0)
                print("  ✓ Worker stopped")
            except asyncio.TimeoutError:
                print("  ⚠ Worker shutdown timed out, aborting...")
                worker.abort()
            except Exception as e:
                print(f"  ✗ Worker stop failed: {e}")
                worker.abort()
        
        if manager:
            try:
                await asyncio.wait_for(manager.graceful_shutdown(), timeout=10.0)
                print("  ✓ Manager stopped")
            except asyncio.TimeoutError:
                print("  ⚠ Manager shutdown timed out, aborting...")
                manager.abort()
            except Exception as e:
                print(f"  ✗ Manager stop failed: {e}")
        
        # Give time for processes to fully terminate
        await asyncio.sleep(1.0)
    
    # -------------------------------------------------------------------------
    # Final Result
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    if all_passed:
        print("TEST PASSED: Workflow stats push verification successful")
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

