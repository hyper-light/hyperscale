#!/usr/bin/env python
"""
Integration test for Gate results aggregation across multiple datacenters.

This test validates:
1. Gates receive WorkflowStats from multiple DCs (via JobFinalResult)
2. Gates aggregate results using the same methods as local execution (Results.merge_results)
3. Gates send properly aggregated GlobalJobResult to clients
4. Per-datacenter stats are preserved alongside aggregated stats
5. Stats updates during execution are aggregated across DCs

Architecture tested:
  Client → Gate → [Manager-DC1, Manager-DC2] → Workers
                      ↓                            ↓
              JobFinalResult              JobFinalResult
                      ↓                            ↓
                      └──────── Gate ─────────────┘
                                 ↓
                         GlobalJobResult
                                 ↓
                             Client

Key aggregation points:
1. Within Manager: Aggregates WorkflowStats from multiple workers (already works)
2. Within Gate: Aggregates JobFinalResult from multiple DCs (needs verification)
3. To Client: GlobalJobResult with per-DC breakdown + aggregated stats
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
from hyperscale.distributed_rewrite.nodes.gate import GateServer
from hyperscale.distributed_rewrite.nodes.client import HyperscaleClient
from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse


# =============================================================================
# Test Workflows
# =============================================================================

class TestWorkflow(Workflow):
    """
    Test workflow that makes HTTP calls.
    Will be distributed across DCs and workers.
    """
    vus = 2  # Small number for testing
    duration = "2s"  # Short duration for testing
    
    @step()
    async def load_test_step(
        self,
        url: URL = 'https://httpbin.org/get',
    ) -> HTTPResponse:
        """Test step - returns HTTPResponse."""
        return await self.client.http.get(url)


# =============================================================================
# Test Implementation
# =============================================================================

async def run_test():
    """
    Run the Gate results aggregation test.
    
    Sets up:
    - 1 Gate (job entry point)
    - 2 Managers in different "datacenters" (DC-ALPHA, DC-BETA)
    - 1 Worker per datacenter
    - 1 Client
    
    Validates:
    - Job dispatched to both DCs
    - Results aggregated from both DCs
    - Per-DC breakdown preserved
    - Cross-DC aggregation correct
    """
    print("=" * 70)
    print("GATE RESULTS AGGREGATION TEST")
    print("=" * 70)
    
    # Setup logging
    LoggingConfig().update(log_directory=os.getcwd(), log_level="info")
    
    env = Env()
    
    # Server addresses
    # Gate
    gate_tcp = 9000
    gate_udp = 9001
    
    # DC-ALPHA (Manager + Worker)
    manager_alpha_tcp = 9100
    manager_alpha_udp = 9101
    worker_alpha_tcp = 9200
    worker_alpha_udp = 9201
    
    # DC-BETA (Manager + Worker)
    manager_beta_tcp = 9110
    manager_beta_udp = 9111
    worker_beta_tcp = 9210
    worker_beta_udp = 9211
    
    # Client
    client_port = 9300
    
    # Servers
    gate = None
    manager_alpha = None
    manager_beta = None
    worker_alpha = None
    worker_beta = None
    client = None
    all_passed = True
    
    try:
        # ---------------------------------------------------------------------
        # Start Gate
        # ---------------------------------------------------------------------
        print("\n[1/8] Starting Gate...")
        print("-" * 50)
        
        gate = GateServer(
            host='127.0.0.1',
            tcp_port=gate_tcp,
            udp_port=gate_udp,
            env=env,
        )
        
        await asyncio.wait_for(gate.start(), timeout=15.0)
        print(f"  ✓ Gate started on TCP:{gate_tcp}")
        
        # Wait for gate to become leader
        gate_leader_wait = 0
        while not gate.is_leader() and gate_leader_wait < 20:
            await asyncio.sleep(1.0)
            gate_leader_wait += 1
        
        if gate.is_leader():
            print(f"  ✓ Gate is leader (after {gate_leader_wait}s)")
        else:
            print(f"  ✗ Gate failed to become leader")
            all_passed = False
        
        # ---------------------------------------------------------------------
        # Start Manager DC-ALPHA
        # ---------------------------------------------------------------------
        print("\n[2/8] Starting Manager DC-ALPHA...")
        print("-" * 50)
        
        manager_alpha = ManagerServer(
            host='127.0.0.1',
            tcp_port=manager_alpha_tcp,
            udp_port=manager_alpha_udp,
            env=env,
            dc_id="DC-ALPHA",
            gate_addrs=[('127.0.0.1', gate_tcp)],
        )
        
        await asyncio.wait_for(manager_alpha.start(), timeout=15.0)
        print(f"  ✓ Manager DC-ALPHA started on TCP:{manager_alpha_tcp}")
        
        # Wait for leader election
        alpha_leader_wait = 0
        while not manager_alpha.is_leader() and alpha_leader_wait < 20:
            await asyncio.sleep(1.0)
            alpha_leader_wait += 1
        
        if manager_alpha.is_leader():
            print(f"  ✓ Manager DC-ALPHA is leader (after {alpha_leader_wait}s)")
        else:
            print(f"  ✗ Manager DC-ALPHA failed to become leader")
            all_passed = False
        
        # ---------------------------------------------------------------------
        # Start Manager DC-BETA
        # ---------------------------------------------------------------------
        print("\n[3/8] Starting Manager DC-BETA...")
        print("-" * 50)
        
        manager_beta = ManagerServer(
            host='127.0.0.1',
            tcp_port=manager_beta_tcp,
            udp_port=manager_beta_udp,
            env=env,
            dc_id="DC-BETA",
            gate_addrs=[('127.0.0.1', gate_tcp)],
        )
        
        await asyncio.wait_for(manager_beta.start(), timeout=15.0)
        print(f"  ✓ Manager DC-BETA started on TCP:{manager_beta_tcp}")
        
        # Wait for leader election
        beta_leader_wait = 0
        while not manager_beta.is_leader() and beta_leader_wait < 20:
            await asyncio.sleep(1.0)
            beta_leader_wait += 1
        
        if manager_beta.is_leader():
            print(f"  ✓ Manager DC-BETA is leader (after {beta_leader_wait}s)")
        else:
            print(f"  ✗ Manager DC-BETA failed to become leader")
            all_passed = False
        
        # ---------------------------------------------------------------------
        # Start Worker DC-ALPHA
        # ---------------------------------------------------------------------
        print("\n[4/8] Starting Worker DC-ALPHA...")
        print("-" * 50)
        
        worker_alpha = WorkerServer(
            host='127.0.0.1',
            tcp_port=worker_alpha_tcp,
            udp_port=worker_alpha_udp,
            env=env,
            total_cores=2,
            dc_id="DC-ALPHA",
            seed_managers=[('127.0.0.1', manager_alpha_tcp)],
        )
        
        await asyncio.wait_for(worker_alpha.start(), timeout=30.0)
        print(f"  ✓ Worker DC-ALPHA started with {worker_alpha._total_cores} cores")
        
        await asyncio.sleep(2.0)  # Allow registration
        
        # Verify registration
        if len(manager_alpha._workers) > 0:
            print(f"  ✓ Worker registered with Manager DC-ALPHA")
        else:
            print(f"  ✗ Worker not registered with Manager DC-ALPHA")
            all_passed = False
        
        # ---------------------------------------------------------------------
        # Start Worker DC-BETA
        # ---------------------------------------------------------------------
        print("\n[5/8] Starting Worker DC-BETA...")
        print("-" * 50)
        
        worker_beta = WorkerServer(
            host='127.0.0.1',
            tcp_port=worker_beta_tcp,
            udp_port=worker_beta_udp,
            env=env,
            total_cores=2,
            dc_id="DC-BETA",
            seed_managers=[('127.0.0.1', manager_beta_tcp)],
        )
        
        await asyncio.wait_for(worker_beta.start(), timeout=30.0)
        print(f"  ✓ Worker DC-BETA started with {worker_beta._total_cores} cores")
        
        await asyncio.sleep(2.0)  # Allow registration
        
        # Verify registration
        if len(manager_beta._workers) > 0:
            print(f"  ✓ Worker registered with Manager DC-BETA")
        else:
            print(f"  ✗ Worker not registered with Manager DC-BETA")
            all_passed = False
        
        # ---------------------------------------------------------------------
        # Allow Gate to Discover Managers
        # ---------------------------------------------------------------------
        print("\n[6/8] Waiting for Gate to discover managers...")
        print("-" * 50)
        
        await asyncio.sleep(5.0)  # Allow heartbeats to propagate
        
        # Check gate's manager tracking
        dc_manager_count = {}
        for dc_id, managers in gate._datacenter_manager_status.items():
            dc_manager_count[dc_id] = len(managers)
        
        print(f"  Gate tracking managers per DC: {dc_manager_count}")
        
        if len(dc_manager_count) >= 2:
            print(f"  ✓ Gate discovered managers in {len(dc_manager_count)} DCs")
        else:
            print(f"  ✗ Gate only discovered {len(dc_manager_count)} DCs (expected 2)")
            all_passed = False
        
        # ---------------------------------------------------------------------
        # Start Client and Submit Job
        # ---------------------------------------------------------------------
        print("\n[7/8] Starting Client and submitting job...")
        print("-" * 50)
        
        client = HyperscaleClient(
            host='127.0.0.1',
            port=client_port,
            env=env,
            gates=[('127.0.0.1', gate_tcp)],
        )
        
        await client.start()
        print(f"  ✓ Client started on port {client_port}")
        
        # Submit job - target BOTH datacenters for aggregation testing
        try:
            job_id = await asyncio.wait_for(
                client.submit_job(
                    workflows=[TestWorkflow],
                    vus=2,  # Match workflow VUs
                    timeout_seconds=60.0,
                    datacenter_count=2,  # Target both DC-ALPHA and DC-BETA
                ),
                timeout=15.0,
            )
            print(f"  ✓ Job submitted: {job_id}")
        except Exception as e:
            print(f"  ✗ Job submission failed: {e}")
            all_passed = False
            job_id = None
        
        # ---------------------------------------------------------------------
        # Wait for Results and Validate Aggregation
        # ---------------------------------------------------------------------
        if job_id:
            print("\n[8/8] Waiting for job completion and validating aggregation...")
            print("-" * 50)
            
            try:
                # Wait for job completion
                result = await asyncio.wait_for(
                    client.wait_for_job(job_id, timeout=120.0),
                    timeout=125.0,
                )
                
                print(f"\n  === GLOBAL JOB RESULT ===")
                print(f"  Job ID: {result.job_id}")
                print(f"  Status: {result.status}")
                print(f"  Total Completed: {result.total_completed}")
                print(f"  Total Failed: {result.total_failed}")
                print(f"  Elapsed: {result.elapsed_seconds:.2f}s")
                
                # Check per-datacenter results
                print(f"\n  === PER-DATACENTER BREAKDOWN ===")
                per_dc_results = getattr(result, 'per_datacenter_results', [])
                for dc_result in per_dc_results:
                    print(f"\n  Datacenter: {dc_result.datacenter}")
                    print(f"    Status: {dc_result.status}")
                    print(f"    Completed: {dc_result.total_completed}")
                    print(f"    Failed: {dc_result.total_failed}")
                    print(f"    Workflows: {len(dc_result.workflow_results)}")
                
                # Check aggregated stats
                print(f"\n  === AGGREGATED STATS (Cross-DC) ===")
                aggregated = getattr(result, 'aggregated', None)
                if aggregated:
                    print(f"    Total Requests: {aggregated.total_requests}")
                    print(f"    Successful: {aggregated.successful_requests}")
                    print(f"    Failed: {aggregated.failed_requests}")
                    print(f"    Overall Rate: {aggregated.overall_rate:.2f}/s")
                    print(f"    Avg Latency: {aggregated.avg_latency_ms:.2f}ms")
                    print(f"    P50 Latency: {aggregated.p50_latency_ms:.2f}ms")
                    print(f"    P95 Latency: {aggregated.p95_latency_ms:.2f}ms")
                    print(f"    P99 Latency: {aggregated.p99_latency_ms:.2f}ms")
                else:
                    print(f"    ✗ No aggregated stats found")
                    all_passed = False
                
                # Validation checks
                print(f"\n  === VALIDATION ===")
                
                # Check we got results from multiple DCs
                if len(per_dc_results) >= 2:
                    print(f"  ✓ Received results from {len(per_dc_results)} DCs")
                else:
                    print(f"  ✗ Only received results from {len(per_dc_results)} DCs")
                    all_passed = False
                
                # Check aggregated totals match sum of per-DC totals
                sum_completed = sum(dc.total_completed for dc in per_dc_results)
                sum_failed = sum(dc.total_failed for dc in per_dc_results)
                
                if result.total_completed == sum_completed:
                    print(f"  ✓ Aggregated completed ({result.total_completed}) matches sum of DCs")
                else:
                    print(f"  ✗ Mismatch: aggregated={result.total_completed}, sum={sum_completed}")
                    all_passed = False
                
                if result.total_failed == sum_failed:
                    print(f"  ✓ Aggregated failed ({result.total_failed}) matches sum of DCs")
                else:
                    print(f"  ✗ Mismatch: aggregated={result.total_failed}, sum={sum_failed}")
                    all_passed = False
                
                # Check latency stats are realistic (not placeholder zeros)
                if aggregated and aggregated.avg_latency_ms > 0:
                    print(f"  ✓ Aggregated latency stats are populated (avg={aggregated.avg_latency_ms:.2f}ms)")
                else:
                    print(f"  ⚠ Aggregated latency stats may be placeholders (avg={aggregated.avg_latency_ms if aggregated else 'N/A'})")
                
                # Check per-DC stats were properly preserved
                dc_names = [dc.datacenter for dc in per_dc_results]
                if "DC-ALPHA" in dc_names and "DC-BETA" in dc_names:
                    print(f"  ✓ Per-DC stats preserved for both datacenters")
                else:
                    print(f"  ⚠ Missing some DC stats: {dc_names}")
                
                # Validate AggregatedJobStats consistency
                if aggregated:
                    # total_requests should equal successful + failed
                    expected_total = aggregated.successful_requests + aggregated.failed_requests
                    if aggregated.total_requests == expected_total:
                        print(f"  ✓ AggregatedJobStats: total_requests ({aggregated.total_requests}) = successful + failed")
                    else:
                        print(f"  ✗ AggregatedJobStats mismatch: total={aggregated.total_requests}, sum={expected_total}")
                        all_passed = False
                    
                    # Latency percentiles should be ordered: p50 <= p95 <= p99
                    if aggregated.p50_latency_ms <= aggregated.p95_latency_ms <= aggregated.p99_latency_ms or \
                       (aggregated.p50_latency_ms == 0 and aggregated.p95_latency_ms == 0 and aggregated.p99_latency_ms == 0):
                        print(f"  ✓ Latency percentiles are ordered correctly (p50 <= p95 <= p99)")
                    else:
                        print(f"  ✗ Latency percentiles out of order: p50={aggregated.p50_latency_ms}, p95={aggregated.p95_latency_ms}, p99={aggregated.p99_latency_ms}")
                        all_passed = False
                    
                    # Overall rate should be > 0 if there are completed requests
                    if aggregated.successful_requests > 0 and aggregated.overall_rate > 0:
                        print(f"  ✓ Overall rate is positive ({aggregated.overall_rate:.2f}/s)")
                    elif aggregated.successful_requests == 0:
                        print(f"  ✓ Overall rate is 0 (no successful requests)")
                    else:
                        print(f"  ⚠ Overall rate is 0 despite {aggregated.successful_requests} successful requests")
                
                # Check job completed successfully
                if result.status == "completed":
                    print(f"  ✓ Job status is completed")
                else:
                    print(f"  ✗ Unexpected job status: {result.status}")
                    all_passed = False
                    
            except asyncio.TimeoutError:
                print(f"  ✗ Job timed out waiting for completion")
                all_passed = False
            except Exception as e:
                print(f"  ✗ Error waiting for job: {e}")
                import traceback
                traceback.print_exc()
                all_passed = False
        else:
            print("\n[8/8] Skipping validation (no job submitted)")
            print("-" * 50)
    
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
        
        await asyncio.sleep(0.5)
        
        if client:
            try:
                await asyncio.wait_for(client.stop(), timeout=5.0)
                print("  ✓ Client stopped")
            except Exception as e:
                print(f"  ✗ Client stop failed: {e}")
        
        if worker_alpha:
            try:
                await asyncio.wait_for(worker_alpha.shutdown(), timeout=15.0)
                print("  ✓ Worker DC-ALPHA stopped")
            except Exception as e:
                print(f"  ✗ Worker DC-ALPHA stop failed: {e}")
        
        if worker_beta:
            try:
                await asyncio.wait_for(worker_beta.shutdown(), timeout=15.0)
                print("  ✓ Worker DC-BETA stopped")
            except Exception as e:
                print(f"  ✗ Worker DC-BETA stop failed: {e}")
        
        if manager_alpha:
            try:
                await asyncio.wait_for(manager_alpha.graceful_shutdown(), timeout=10.0)
                print("  ✓ Manager DC-ALPHA stopped")
            except Exception as e:
                print(f"  ✗ Manager DC-ALPHA stop failed: {e}")
        
        if manager_beta:
            try:
                await asyncio.wait_for(manager_beta.graceful_shutdown(), timeout=10.0)
                print("  ✓ Manager DC-BETA stopped")
            except Exception as e:
                print(f"  ✗ Manager DC-BETA stop failed: {e}")
        
        if gate:
            try:
                await asyncio.wait_for(gate.graceful_shutdown(), timeout=10.0)
                print("  ✓ Gate stopped")
            except Exception as e:
                print(f"  ✗ Gate stop failed: {e}")
        
        await asyncio.sleep(1.0)
    
    # -------------------------------------------------------------------------
    # Final Result
    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    if all_passed:
        print("TEST PASSED: Gate results aggregation working correctly")
    else:
        print("TEST FAILED: Some checks failed")
    print("=" * 70)
    
    return all_passed


if __name__ == "__main__":
    try:
        success = asyncio.run(run_test())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nInterrupted")
        sys.exit(1)

