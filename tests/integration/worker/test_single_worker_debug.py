#!/usr/bin/env python
"""
Debug test to isolate where worker startup hangs.
"""

import asyncio
import os
import sys

import pytest

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.logging.config import LoggingConfig
from hyperscale.distributed.env.env import Env
from hyperscale.distributed.nodes.worker import WorkerServer


@pytest.mark.skip(reason="Debug test that spawns actual processes - run manually only")
async def test_worker_startup_phases():
    """Test worker startup in phases to find where it hangs."""
    
    # Setup logging
    LoggingConfig().update(log_directory=os.getcwd(), log_level="debug")
    
    env = Env()
    
    # Set WORKER_MAX_CORES via env
    env.WORKER_MAX_CORES = 2

    worker = WorkerServer(
        host='127.0.0.1',
        tcp_port=9200,
        udp_port=9201,
        env=env,
        dc_id="DC-TEST",
        seed_managers=[],  # No managers
    )
    
    print("[1/8] Worker created")
    print(f"  - _local_udp_port: {worker._local_udp_port}")
    print(f"  - _total_cores: {worker._total_cores}")
    
    # Phase 1: Calculate worker IPs
    print("\n[2/8] Calculating worker IPs...")
    worker_ips = worker._bin_and_check_socket_range()
    print(f"  ✓ Worker IPs: {worker_ips}")
    
    # Phase 2: Start CPU monitor
    print("\n[3/8] Starting CPU monitor...")
    await asyncio.wait_for(
        worker._cpu_monitor.start_background_monitor(
            worker._node_id.datacenter,
            worker._node_id.full,
        ),
        timeout=5.0
    )
    print("  ✓ CPU monitor started")
    
    # Phase 3: Start memory monitor
    print("\n[4/8] Starting memory monitor...")
    await asyncio.wait_for(
        worker._memory_monitor.start_background_monitor(
            worker._node_id.datacenter,
            worker._node_id.full,
        ),
        timeout=5.0
    )
    print("  ✓ Memory monitor started")
    
    # Phase 4: Setup server pool
    print("\n[5/8] Setting up server pool...")
    try:
        await asyncio.wait_for(
            worker._server_pool.setup(),
            timeout=10.0
        )
        print("  ✓ Server pool setup complete")
    except asyncio.TimeoutError:
        print("  ✗ TIMEOUT: Server pool setup hung!")
        return
    
    # Phase 5: Start remote manager
    print("\n[6/8] Starting remote manager...")
    try:
        await asyncio.wait_for(
            worker._remote_manger.start(
                worker._host,
                worker._local_udp_port,
                worker._local_env,
            ),
            timeout=10.0
        )
        print("  ✓ Remote manager started")
    except asyncio.TimeoutError:
        print("  ✗ TIMEOUT: Remote manager start hung!")
        return
    
    # Phase 6: Run pool (spawns worker processes)
    print("\n[7/8] Running server pool...")
    try:
        await asyncio.wait_for(
            worker._server_pool.run_pool(
                (worker._host, worker._local_udp_port),
                worker_ips,
                worker._local_env,
            ),
            timeout=10.0
        )
        print("  ✓ Server pool running")
    except asyncio.TimeoutError:
        print("  ✗ TIMEOUT: Server pool run_pool hung!")
        return
    
    # Phase 7: Connect to workers (THIS IS LIKELY THE HANG)
    print("\n[8/8] Connecting to workers...")
    print("  Note: This calls poll_for_start which has NO TIMEOUT!")
    try:
        await asyncio.wait_for(
            worker._remote_manger.connect_to_workers(
                worker_ips,
                timeout=5.0,  # This timeout is for individual operations, not poll_for_start
            ),
            timeout=15.0  # Outer timeout
        )
        print("  ✓ Connected to workers")
    except asyncio.TimeoutError:
        print("  ✗ TIMEOUT: connect_to_workers hung!")
        print("  ✗ Root cause: poll_for_start() loops forever waiting for worker acknowledgments")
        return
    
    print("\n✓ All phases completed successfully!")
    
    # Cleanup
    await worker.stop()
    print("✓ Worker shutdown")


if __name__ == "__main__":
    try:
        asyncio.run(test_worker_startup_phases())
    except KeyboardInterrupt:
        print("\nInterrupted")

