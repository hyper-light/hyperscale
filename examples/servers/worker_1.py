#!/usr/bin/env python3
"""
Worker Test Server 1

This server runs on ports 9200 (TCP) and 9201 (UDP) and demonstrates
the Worker node with workflow execution capabilities.

Port Allocation:
    Worker 1: TCP 9200, UDP 9201
    Worker 2: TCP 9202, UDP 9203
    Worker 3: TCP 9204, UDP 9205
    Worker 4: TCP 9206, UDP 9207
    Worker 5: TCP 9208, UDP 9209

Usage:
    python worker_1.py

This server will:
1. Start up and register with managers
2. Join SWIM cluster for healthchecks
3. Report capacity (cores available)
4. Receive and execute workflows
5. Report progress back to managers
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.distributed_rewrite.env import Env
from hyperscale.distributed_rewrite.nodes import WorkerServer


async def run_worker_1():
    """Run Worker server 1 on ports 9200/9201"""
    
    print("=" * 60)
    print("Worker Test Server 1")
    print("=" * 60)
    print("TCP Port: 9200")
    print("UDP Port: 9201")
    print("Datacenter: DC-EAST")
    print("=" * 60)
    
    # Seed managers to register with (TCP addresses)
    seed_managers = [
        ('127.0.0.1', 9000),  # Manager 1
        ('127.0.0.1', 9002),  # Manager 2
        ('127.0.0.1', 9004),  # Manager 3
        ('127.0.0.1', 9006),  # Manager 4
        ('127.0.0.1', 9008),  # Manager 5
    ]
    
    server = WorkerServer(
        host='127.0.0.1',
        tcp_port=9200,
        udp_port=9201,
        env=Env(
            MERCURY_SYNC_REQUEST_TIMEOUT='2s',
        ),
        dc_id='DC-EAST',
        total_cores=4,  # Simulated core count
        seed_managers=seed_managers,
    )
    
    await server.start()
    
    print("\n[Worker 1] Started successfully!")
    print(f"[Worker 1] Node ID: {server._node_id}")
    print(f"[Worker 1] Short ID: {server._node_id.short}")
    print(f"[Worker 1] Total Cores: {server._total_cores}")
    print(f"[Worker 1] Available Cores: {server._available_cores}")
    
    # Run status display loop
    try:
        while True:
            await asyncio.sleep(5)
            
            # Display current status
            print("\n" + "-" * 50)
            print(f"[Worker 1] Status Update")
            print("-" * 50)
            print(f"  Node ID: {server._node_id.short}")
            
            # Capacity
            print(f"\n  Capacity:")
            print(f"    Total Cores: {server._total_cores}")
            print(f"    Available: {server._available_cores}")
            print(f"    In Use: {server._total_cores - server._available_cores}")
            
            # Manager status
            print(f"\n  Managers:")
            print(f"    Known: {len(server._known_managers)}")
            print(f"    Healthy: {len(server._healthy_manager_ids)}")
            print(f"    Primary: {server._primary_manager_id}")
            
            # Circuit breaker status
            circuit = server.get_manager_circuit_status()
            print(f"\n  Circuit Breaker:")
            print(f"    State: {circuit['circuit_state']}")
            print(f"    Errors: {circuit['error_count']}")
            
            # Workflow status
            print(f"\n  Workflows:")
            print(f"    Active: {len(server._workflows)}")
            for wf_id in list(server._workflows.keys())[:3]:
                wf = server._workflows[wf_id]
                print(f"    - {wf_id[:20]}... status={wf.status}")
            
    except asyncio.CancelledError:
        print("\n[Worker 1] Shutting down...")
        await server.stop()
        print("[Worker 1] Shutdown complete")


if __name__ == '__main__':
    try:
        asyncio.run(run_worker_1())
    except KeyboardInterrupt:
        print("\n[Worker 1] Interrupted by user")

