#!/usr/bin/env python3
"""
Manager Test Server 1

This server runs on ports 9000 (TCP) and 9001 (UDP) and demonstrates
the Manager node with SWIM-based leader election and worker management.

Port Allocation:
    Manager 1: TCP 9000, UDP 9001
    Manager 2: TCP 9002, UDP 9003
    Manager 3: TCP 9004, UDP 9005
    Manager 4: TCP 9006, UDP 9007
    Manager 5: TCP 9008, UDP 9009

Usage:
    python manager_1.py

This server will:
1. Start up in SYNCING state
2. Join SWIM cluster with other managers
3. Participate in leader election
4. Accept worker registrations
5. Dispatch workflows to workers
"""

import asyncio
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes import ManagerServer


async def run_manager_1():
    """Run Manager server 1 on ports 9000/9001"""
    
    print("=" * 60)
    print("Manager Test Server 1")
    print("=" * 60)
    print("TCP Port: 9000")
    print("UDP Port: 9001")
    print("Datacenter: DC-EAST")
    print("=" * 60)
    
    # Manager peer addresses (TCP for quorum, UDP for SWIM)
    manager_peers_tcp = [
        ('127.0.0.1', 9002),  # Manager 2
        ('127.0.0.1', 9004),  # Manager 3
        ('127.0.0.1', 9006),  # Manager 4
        ('127.0.0.1', 9008),  # Manager 5
    ]
    manager_peers_udp = [
        ('127.0.0.1', 9003),  # Manager 2
        ('127.0.0.1', 9005),  # Manager 3
        ('127.0.0.1', 9007),  # Manager 4
        ('127.0.0.1', 9009),  # Manager 5
    ]
    
    # Gate addresses (optional - if running gates)
    gate_addrs_tcp = [
        ('127.0.0.1', 9100),  # Gate 1
        ('127.0.0.1', 9102),  # Gate 2
        ('127.0.0.1', 9104),  # Gate 3
    ]
    gate_addrs_udp = [
        ('127.0.0.1', 9101),  # Gate 1
        ('127.0.0.1', 9103),  # Gate 2
        ('127.0.0.1', 9105),  # Gate 3
    ]
    
    server = ManagerServer(
        host='127.0.0.1',
        tcp_port=9000,
        udp_port=9001,
        env=Env(
            MERCURY_SYNC_REQUEST_TIMEOUT='2s',
        ),
        dc_id='DC-EAST',
        manager_peers=manager_peers_tcp,
        manager_udp_peers=manager_peers_udp,
        gate_addrs=gate_addrs_tcp,
        gate_udp_addrs=gate_addrs_udp,
        quorum_timeout=5.0,
    )
    
    await server.start()
    
    print("\n[Manager 1] Started successfully!")
    print(f"[Manager 1] Node ID: {server._node_id}")
    print(f"[Manager 1] Short ID: {server._node_id.short}")
    print(f"[Manager 1] State: {server._manager_state.value}")
    
    # Run status display loop
    try:
        while True:
            await asyncio.sleep(5)
            
            # Display current status
            print("\n" + "-" * 50)
            print(f"[Manager 1] Status Update")
            print("-" * 50)
            print(f"  Node ID: {server._node_id.short}")
            print(f"  State: {server._manager_state.value}")
            
            # Leadership status
            print(f"\n  Leadership:")
            print(f"    Is Leader: {server.is_leader()}")
            leader = server.get_current_leader()
            print(f"    Current Leader: {leader}")
            
            # Quorum status
            quorum = server.get_quorum_status()
            print(f"\n  Quorum:")
            print(f"    Active Managers: {quorum['active_managers']}")
            print(f"    Required: {quorum['required_quorum']}")
            print(f"    Available: {quorum['quorum_available']}")
            print(f"    Circuit: {quorum['circuit_state']}")
            
            # Worker status
            print(f"\n  Workers:")
            print(f"    Registered: {len(server._workers)}")
            print(f"    Healthy: {len(server._healthy_worker_ids)}")
            for worker_id in list(server._workers.keys())[:5]:
                worker = server._workers[worker_id]
                print(f"    - {worker_id[:20]}... cores={worker.available_cores}/{worker.total_cores}")
            
            # Job status
            print(f"\n  Jobs:")
            print(f"    Active: {len(server._jobs)}")
            print(f"    Workflows: {len(server._workflows)}")
            
    except asyncio.CancelledError:
        print("\n[Manager 1] Shutting down...")
        await server.stop()
        print("[Manager 1] Shutdown complete")


if __name__ == '__main__':
    try:
        asyncio.run(run_manager_1())
    except KeyboardInterrupt:
        print("\n[Manager 1] Interrupted by user")

