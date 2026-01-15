#!/usr/bin/env python3
"""
Single Worker Startup/Shutdown Test.

Tests that:
1. A single worker with 8 CPUs starts correctly
2. The worker shuts down cleanly without errors

This is a basic sanity test before more complex integration tests.
"""

import asyncio
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.distributed.nodes.worker import WorkerServer
from hyperscale.distributed.env.env import Env
from hyperscale.logging.config.logging_config import LoggingConfig

# Initialize logging directory (required for server pool)
_logging_config = LoggingConfig()
_logging_config.update(log_directory=os.getcwd())


# ==========================================================================
# Configuration
# ==========================================================================

DC_ID = "DC-TEST"
WORKER_TCP_PORT = 9200
WORKER_UDP_PORT = 9201
WORKER_CORES = 8

# No seed managers for this standalone test
SEED_MANAGERS: list[tuple[str, int]] = []


async def run_test():
    """Run the single worker startup/shutdown test."""
    
    worker: WorkerServer | None = None
    
    try:
        # ==============================================================
        # STEP 1: Create worker
        # ==============================================================
        print("[1/4] Creating worker with 8 CPUs...")
        print("-" * 50)
        
        worker = WorkerServer(
            host='127.0.0.1',
            tcp_port=WORKER_TCP_PORT,
            udp_port=WORKER_UDP_PORT,
            env=Env(MERCURY_SYNC_REQUEST_TIMEOUT='2s'),
            dc_id=DC_ID,
            total_cores=WORKER_CORES,
            seed_managers=SEED_MANAGERS,
        )
        
        print(f"  ✓ Worker created")
        print(f"    - TCP Port: {WORKER_TCP_PORT}")
        print(f"    - UDP Port: {WORKER_UDP_PORT}")
        print(f"    - Total Cores: {WORKER_CORES}")
        print(f"    - Datacenter: {DC_ID}")
        print()
        
        # ==============================================================
        # STEP 2: Start worker
        # ==============================================================
        print("[2/4] Starting worker...")
        print("-" * 50)
        
        await worker.start()
        
        print(f"  ✓ Worker started")
        print(f"    - Node ID: {worker._node_id.short}")
        print(f"    - Available Cores: {worker._available_cores}")
        print(f"    - Running: {worker._running}")
        print()
        
        # ==============================================================
        # STEP 3: Verify worker state
        # ==============================================================
        print("[3/4] Verifying worker state...")
        print("-" * 50)
        
        # Check core counts
        if worker._total_cores == WORKER_CORES:
            print(f"  ✓ Total cores correct: {worker._total_cores}")
        else:
            print(f"  ✗ Total cores mismatch: expected {WORKER_CORES}, got {worker._total_cores}")
            return False
        
        if worker._available_cores == WORKER_CORES:
            print(f"  ✓ Available cores correct: {worker._available_cores}")
        else:
            print(f"  ✗ Available cores mismatch: expected {WORKER_CORES}, got {worker._available_cores}")
            return False
        
        # Check running state
        if worker._running:
            print(f"  ✓ Worker is running")
        else:
            print(f"  ✗ Worker is not running")
            return False
        
        # Check no active workflows
        if len(worker._active_workflows) == 0:
            print(f"  ✓ No active workflows (expected)")
        else:
            print(f"  ✗ Unexpected active workflows: {len(worker._active_workflows)}")
            return False
        
        print()
        
        # ==============================================================
        # STEP 4: Shutdown worker
        # ==============================================================
        print("[4/4] Shutting down worker...")
        print("-" * 50)
        
        await worker.stop()
        
        print(f"  ✓ Worker shutdown complete")
        print()
        
        # ==============================================================
        # SUCCESS
        # ==============================================================
        print("=" * 50)
        print("TEST PASSED: Single worker startup/shutdown successful")
        print("=" * 50)
        return True
        
    except Exception as e:
        print(f"\n✗ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Cleanup
        if worker is not None:
            try:
                await worker.stop()
            except Exception:
                pass


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

