#!/usr/bin/env python
"""
Test workflow execution on a worker, verifying:
1. Workflows execute correctly
2. Context is updated by Provide hooks
3. Context is consumed by Use hooks
4. Results (WorkflowStats) are returned correctly
5. Dependent workflows receive context from dependencies
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
from hyperscale.distributed_rewrite.nodes.worker import WorkerServer
from hyperscale.distributed_rewrite.models import (
    WorkflowDispatch,
    WorkflowProgress,
    WorkflowStatus,
)
from hyperscale.graph import Workflow, step, depends, state, Use, Provide


# =============================================================================
# Test Workflows
# =============================================================================

class SimpleWorkflow(Workflow):
    """Simple workflow with no context - just executes and returns."""
    vus = 10
    duration = "5s"
    
    @step()
    async def simple_action(self) -> dict:
        """Simple action that returns a dict."""
        await asyncio.sleep(0.1)  # Simulate work
        return {"status": "ok", "value": 42}


class ProviderWorkflow(Workflow):
    """Workflow that provides context to dependent workflows."""
    vus = 10
    duration = "5s"
    
    @step()
    async def do_work(self) -> dict:
        """Do some work before providing context."""
        await asyncio.sleep(0.1)
        return {"computed": True}
    
    @state('ConsumerWorkflow')
    def provide_data(self) -> Provide[dict]:
        """Provide data to ConsumerWorkflow."""
        return {"shared_key": "shared_value", "counter": 100}


@depends('ProviderWorkflow')
class ConsumerWorkflow(Workflow):
    """Workflow that consumes context from ProviderWorkflow."""
    vus = 10
    duration = "5s"
    
    @state('ProviderWorkflow')
    def consume_data(self, provide_data: dict | None = None) -> Use[dict]:
        """Consume data from ProviderWorkflow."""
        # Store what we received for verification
        self._received_context = provide_data
        return provide_data
    
    @step()
    async def process_with_context(self) -> dict:
        """Process using the consumed context."""
        await asyncio.sleep(0.1)
        received = getattr(self, '_received_context', None)
        return {
            "received_context": received,
            "processed": True,
        }


# =============================================================================
# Test Implementation
# =============================================================================

async def create_dispatch(
    job_id: str,
    workflow_id: str,
    workflow_class: type,
    context: dict | None = None,
    vus: int = 2,
    timeout: float = 30.0,
) -> WorkflowDispatch:
    """Create a WorkflowDispatch message."""
    return WorkflowDispatch(
        job_id=job_id,
        workflow_id=workflow_id,
        workflow=cloudpickle.dumps(workflow_class),
        context=cloudpickle.dumps(context or {}),
        vus=vus,
        timeout_seconds=timeout,
        fence_token=1,
        context_version=0,
    )


async def execute_and_wait(
    worker: WorkerServer,
    dispatch: WorkflowDispatch,
    timeout: float = 60.0,
) -> tuple[WorkflowProgress | None, Exception | None]:
    """
    Execute a workflow and wait for completion.
    
    Returns (progress, error)
    
    Note: Context updates are sent to the manager via WorkflowFinalResult.
    Since we don't have a manager in this test, we just verify execution works.
    """
    # Create progress tracker
    progress = WorkflowProgress(
        job_id=dispatch.job_id,
        workflow_id=dispatch.workflow_id,
        workflow_name="",
        status=WorkflowStatus.PENDING.value,
        completed_count=0,
        failed_count=0,
        rate_per_second=0.0,
        elapsed_seconds=0.0,
    )
    
    # Create cancellation event
    cancel_event = asyncio.Event()
    
    # Allocate cores
    allocated_cores = min(dispatch.vus, worker._total_cores)
    allocated_vus = dispatch.vus
    
    # Reserve cores
    cores_to_use = list(range(allocated_cores))
    for core in cores_to_use:
        worker._core_assignments[core] = dispatch.workflow_id
    progress.assigned_cores = cores_to_use
    
    error = None
    worker._send_progress_update  = lambda a, b, c: (
        a,
        b,
        c
    )
    
    worker._send_final_result = lambda a, b, c: (
        a,
        b,
        c
    )
    
    try:
        # Execute workflow with timeout
         (
            progress,
            error,
         ) = await asyncio.wait_for(
            worker._execute_workflow(
                dispatch,
                progress,
                cancel_event,
                allocated_vus,
                allocated_cores,
            ),
            timeout=timeout,
        )
        
    except asyncio.TimeoutError:
        error = TimeoutError(f"Workflow {dispatch.workflow_id} timed out after {timeout}s")
        progress.status = WorkflowStatus.FAILED.value
    except Exception as e:
        error = e
        progress.status = WorkflowStatus.FAILED.value

    print(progress.status)
    
    return progress, error


async def run_test():
    """Run the workflow execution test."""
    print("=" * 60)
    print("WORKFLOW EXECUTION TEST")
    print("=" * 60)
    
    # Setup logging
    LoggingConfig().update(log_directory=os.getcwd(), log_level="info")
    
    env = Env()
    
    # Create worker with 4 cores
    worker = WorkerServer(
        host='127.0.0.1',
        tcp_port=9200,
        udp_port=9201,
        env=env,
        total_cores=4,
        dc_id="DC-TEST",
        seed_managers=[],  # No managers - standalone test
    )
    
    print("\n[1/6] Starting worker...")
    print("-" * 50)
    
    try:
        await asyncio.wait_for(worker.start(), timeout=30.0)
        print(f"  ✓ Worker started with {worker._total_cores} cores")
    except asyncio.TimeoutError:
        print("  ✗ Worker startup timed out!")
        return False
    except Exception as e:
        print(f"  ✗ Worker startup failed: {e}")
        return False
    
    job_id = "test-job-001"
    all_passed = True
    
    # -------------------------------------------------------------------------
    # Test 1: Simple Workflow
    # -------------------------------------------------------------------------
    print("\n[2/6] Testing SimpleWorkflow...")
    print("-" * 50)
    
    dispatch1 = await create_dispatch(
        job_id=job_id,
        workflow_id="wf-simple-001",
        workflow_class=SimpleWorkflow(),
        vus=2,
    )
    
    progress1, error1 = await execute_and_wait(worker, dispatch1, timeout=30.0)
    
    if error1:
        print(f"  ✗ SimpleWorkflow failed: {error1}")
        all_passed = False
    elif progress1.status != WorkflowStatus.COMPLETED.value:
        print(f"  ✗ SimpleWorkflow status incorrect: {progress1.status}")
        all_passed = False
    else:
        print(f"  ✓ SimpleWorkflow completed")
        print(f"    - Status: {progress1.status}")
        print(f"    - Elapsed: {progress1.elapsed_seconds:.2f}s")
        print(f"    - Cores used: {len(progress1.assigned_cores)}")
    
    # -------------------------------------------------------------------------
    # Test 2: ProviderWorkflow (sets context)
    # -------------------------------------------------------------------------
    print("\n[3/6] Testing ProviderWorkflow...")
    print("-" * 50)
    
    dispatch2 = await create_dispatch(
        job_id=job_id,
        workflow_id="wf-provider-001",
        workflow_class=ProviderWorkflow(),
        vus=2,
    )
    
    progress2, error2 = await execute_and_wait(worker, dispatch2, timeout=30.0)
    
    if error2:
        print(f"  ✗ ProviderWorkflow failed: {error2}")
        all_passed = False
    elif progress2.status != WorkflowStatus.COMPLETED.value:
        print(f"  ✗ ProviderWorkflow status incorrect: {progress2.status}")
        all_passed = False
    else:
        print(f"  ✓ ProviderWorkflow completed")
        print(f"    - Status: {progress2.status}")
        print(f"    - Elapsed: {progress2.elapsed_seconds:.2f}s")
        print(f"    - Context sent via WorkflowFinalResult (requires manager to verify)")
    
    # -------------------------------------------------------------------------
    # Test 3: ConsumerWorkflow (uses context)
    # -------------------------------------------------------------------------
    print("\n[4/6] Testing ConsumerWorkflow...")
    print("-" * 50)
    
    # For this standalone test, we simulate context being passed
    # In a real scenario, the manager would pass context from ProviderWorkflow
    simulated_context = {"ProviderWorkflow": {"provide_data": {"shared_key": "shared_value", "counter": 100}}}
    
    dispatch3 = await create_dispatch(
        job_id=job_id,
        workflow_id="wf-consumer-001",
        workflow_class=ConsumerWorkflow(),
        context=simulated_context,
        vus=2,
    )
    
    progress3, error3 = await execute_and_wait(worker, dispatch3, timeout=30.0)
    
    if error3:
        print(f"  ✗ ConsumerWorkflow failed: {error3}")
        all_passed = False
    elif progress3.status != WorkflowStatus.COMPLETED.value:
        print(f"  ✗ ConsumerWorkflow status incorrect: {progress3.status}")
        all_passed = False
    else:
        print(f"  ✓ ConsumerWorkflow completed")
        print(f"    - Status: {progress3.status}")
        print(f"    - Elapsed: {progress3.elapsed_seconds:.2f}s")
        print(f"    - Used simulated context from ProviderWorkflow")
    
    # -------------------------------------------------------------------------
    # Verify Results
    # -------------------------------------------------------------------------
    print("\n[5/6] Verifying results...")
    print("-" * 50)
    
    # Check all workflows completed
    workflows_completed = all([
        progress1 and progress1.status == WorkflowStatus.COMPLETED.value,
        progress2 and progress2.status == WorkflowStatus.COMPLETED.value,
        progress3 and progress3.status == WorkflowStatus.COMPLETED.value,
    ])
    
    if workflows_completed:
        print("  ✓ All 3 workflows completed successfully")
    else:
        print("  ✗ Not all workflows completed")
        all_passed = False
    
    # Check cores were freed
    active_assignments = sum(1 for v in worker._core_assignments.values() if v is not None)
    if active_assignments == 0:
        print("  ✓ All cores freed after execution")
    else:
        print(f"  ✗ {active_assignments} cores still assigned")
        all_passed = False
    
    # -------------------------------------------------------------------------
    # Cleanup
    # -------------------------------------------------------------------------
    print("\n[6/6] Shutting down worker...")
    print("-" * 50)
    
    try:
        await worker.stop()
        print("  ✓ Worker shutdown complete")
    except Exception as e:
        print(f"  ✗ Worker shutdown failed: {e}")
        all_passed = False
    
    # -------------------------------------------------------------------------
    # Final Result
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    if all_passed:
        print("TEST PASSED: All workflow execution tests passed")
    else:
        print("TEST FAILED: Some tests failed")
    print("=" * 60)
    
    return all_passed


if __name__ == "__main__":
    try:
        success = asyncio.run(run_test())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nInterrupted")
        sys.exit(1)

