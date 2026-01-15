#!/usr/bin/env python3
"""
Gate Test Server 1

This server runs on ports 9100 (TCP) and 9101 (UDP) and demonstrates
the Gate node with SWIM-based leader election and job routing.

Port Allocation:
    Gate 1: TCP 9100, UDP 9101
    Gate 2: TCP 9102, UDP 9103
    Gate 3: TCP 9104, UDP 9105
    Gate 4: TCP 9106, UDP 9107
    Gate 5: TCP 9108, UDP 9109

Usage:
    python gate_1.py

This server will:
1. Start up in SYNCING state
2. Join SWIM cluster with other gates
3. Participate in leader election
4. Accept job submissions from clients
5. Route jobs to managers in datacenters
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes import GateServer


async def run_gate_1():
    """Run Gate server 1 on ports 9100/9101"""
    
    print("=" * 60)
    print("Gate Test Server 1")
    print("=" * 60)
    print("TCP Port: 9100")
    print("UDP Port: 9101")
    print("Role: Global (spans datacenters)")
    print("=" * 60)
    
    # Gate peer addresses (TCP for state sync, UDP for SWIM)
    gate_peers_tcp = [
        ('127.0.0.1', 9102),  # Gate 2
        ('127.0.0.1', 9104),  # Gate 3
        ('127.0.0.1', 9106),  # Gate 4
        ('127.0.0.1', 9108),  # Gate 5
    ]
    gate_peers_udp = [
        ('127.0.0.1', 9103),  # Gate 2
        ('127.0.0.1', 9105),  # Gate 3
        ('127.0.0.1', 9107),  # Gate 4
        ('127.0.0.1', 9109),  # Gate 5
    ]
    
    # Datacenter manager addresses
    # DC-EAST has 5 managers
    datacenter_managers_tcp = {
        'DC-EAST': [
            ('127.0.0.1', 9000),  # Manager 1
            ('127.0.0.1', 9002),  # Manager 2
            ('127.0.0.1', 9004),  # Manager 3
            ('127.0.0.1', 9006),  # Manager 4
            ('127.0.0.1', 9008),  # Manager 5
        ],
    }
    datacenter_managers_udp = {
        'DC-EAST': [
            ('127.0.0.1', 9001),  # Manager 1
            ('127.0.0.1', 9003),  # Manager 2
            ('127.0.0.1', 9005),  # Manager 3
            ('127.0.0.1', 9007),  # Manager 4
            ('127.0.0.1', 9009),  # Manager 5
        ],
    }
    
    server = GateServer(
        host='127.0.0.1',
        tcp_port=9100,
        udp_port=9101,
        env=Env(
            MERCURY_SYNC_REQUEST_TIMEOUT='2s',
        ),
        dc_id='global',
        gate_peers=gate_peers_tcp,
        gate_udp_peers=gate_peers_udp,
        datacenter_managers=datacenter_managers_tcp,
        datacenter_manager_udp=datacenter_managers_udp,
        lease_timeout=30.0,
    )
    
    await server.start()
    
    print("\n[Gate 1] Started successfully!")
    print(f"[Gate 1] Node ID: {server._node_id}")
    print(f"[Gate 1] Short ID: {server._node_id.short}")
    print(f"[Gate 1] State: {server._gate_state.value}")
    
    # Run status display loop
    try:
        while True:
            await asyncio.sleep(5)
            
            # Display current status
            print("\n" + "-" * 50)
            print(f"[Gate 1] Status Update")
            print("-" * 50)
            print(f"  Node ID: {server._node_id.short}")
            print(f"  State: {server._gate_state.value}")
            
            # Leadership status
            print(f"\n  Leadership:")
            print(f"    Is Leader: {server.is_leader()}")
            leader = server.get_current_leader()
            print(f"    Current Leader: {leader}")
            
            # Quorum status
            quorum = server.get_quorum_status()
            print(f"\n  Quorum:")
            print(f"    Active Gates: {quorum['active_gates']}")
            print(f"    Required: {quorum['required_quorum']}")
            print(f"    Available: {quorum['quorum_available']}")
            print(f"    Circuit: {quorum['circuit_state']}")
            
            # Datacenter status
            print(f"\n  Datacenters:")
            dc_health = server.get_datacenter_health()
            for dc, status in dc_health.items():
                print(f"    {dc}: {status.health}")
            
            # Job status
            print(f"\n  Jobs:")
            print(f"    Active: {len(server._jobs)}")
            print(f"    Leases: {len(server._leases)}")
            
    except asyncio.CancelledError:
        print("\n[Gate 1] Shutting down...")
        await server.stop()
        print("[Gate 1] Shutdown complete")


if __name__ == '__main__':
    try:
        asyncio.run(run_gate_1())
    except KeyboardInterrupt:
        print("\n[Gate 1] Interrupted by user")

