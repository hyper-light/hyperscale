#!/usr/bin/env python3
"""
SWIM + Lifeguard Test Server 1 with Leadership Election

This server runs on ports 8670 (TCP) and 8671 (UDP) and demonstrates
the SWIM protocol with Lifeguard enhancements and hierarchical leadership.

Usage:
    python swim_server_1.py

This server will:
1. Start up and begin the probe cycle
2. Attempt to join server 2 at 127.0.0.1:8673
3. Exchange probes and membership information
4. Participate in leader election
5. Display leadership status
"""

import asyncio
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collections import defaultdict
from hyperscale.distributed_rewrite.env import Env

# Import the SWIM server implementation from the swim package
from swim import UDPServer


async def run_server_1():
    """Run SWIM server 1 on ports 8670/8671"""
    
    print("=" * 60)
    print("SWIM + Lifeguard + Leadership Test Server 1")
    print("=" * 60)
    print("TCP Port: 8670")
    print("UDP Port: 8671")
    print("Datacenter: DC-EAST")
    print("=" * 60)
    
    server = UDPServer(
        '127.0.0.1',
        8670,  # TCP port
        8671,  # UDP port
        Env(
            MERCURY_SYNC_REQUEST_TIMEOUT='2s',
        ),
        dc_id='DC-EAST',
        priority=1,  # High priority for leadership
    )

    await server.start_server(init_context={
        'max_probe_timeout': 10,
        'min_probe_timeout': 1,
        'current_timeout': 2,
        'nodes': defaultdict(asyncio.Queue),
        'udp_poll_interval': 2,
        # Suspicion timeout settings (Lifeguard)
        'suspicion_min_timeout': 2.0,
        'suspicion_max_timeout': 15.0,
    })
    
    print("\n[Server 1] Started successfully!")
    print(f"[Server 1] Node ID: {server.node_id}")
    print(f"[Server 1] Short ID: {server.node_id.short}")
    print(f"[Server 1] Local Health Multiplier: {server._local_health.get_multiplier():.2f}")
    print(f"[Server 1] Incarnation: {server.get_self_incarnation()}")
    
    # Wait a moment for server 2 to potentially start
    await asyncio.sleep(2)
    
    # Try to join server 2
    server_2_addr = ('127.0.0.1', 8673)
    self_udp_addr = ('127.0.0.1', 8671)
    print(f"\n[Server 1] Attempting to join Server 2 at {server_2_addr}...")
    
    try:
        # Send join message with OUR address (the node joining)
        join_msg = b'join>' + f'{self_udp_addr[0]}:{self_udp_addr[1]}'.encode()
        server._task_runner.run(
            server.send,
            server_2_addr,
            join_msg,
            timeout=5,
        )
        print("[Server 1] Join request sent to Server 2")
        
        # Add server 2 to our known nodes
        nodes = server._context.read('nodes')
        nodes[server_2_addr].put_nowait((0, b'OK'))
        server._probe_scheduler.add_member(server_2_addr)
        server._incarnation_tracker.update_node(
            server_2_addr, b'OK', 0, 0
        )
        print(f"[Server 1] Added Server 2 to membership list")
        
    except Exception as e:
        print(f"[Server 1] Failed to join Server 2: {e}")
    
    # Start the probe cycle in the background
    print("\n[Server 1] Starting probe cycle...")
    probe_task = asyncio.create_task(server.start_probe_cycle())
    
    # Start leader election
    print("[Server 1] Starting leader election...")
    election_task = asyncio.create_task(server.start_leader_election())
    
    # Run status display loop
    try:
        while True:
            await asyncio.sleep(5)
            
            # Display current status
            print("\n" + "-" * 50)
            print(f"[{server.node_id.short}] Status Update")
            print("-" * 50)
            print(f"  Node ID: {server.node_id}")
            
            # SWIM status
            print(f"  LHM Score: {server._local_health.score}/{server._local_health.max_score}")
            print(f"  LHM Multiplier: {server._local_health.get_multiplier():.2f}")
            print(f"  Incarnation: {server.get_self_incarnation()}")
            print(f"  Probe Scheduler Members: {len(server._probe_scheduler.members)}")
            print(f"  Active Suspicions: {len(server._suspicion_manager.suspicions)}")
            
            # Leadership status
            leader_status = server.get_leadership_status()
            print(f"\n  Leadership:")
            print(f"    Role: {leader_status['role'].upper()}")
            print(f"    Term: {leader_status['term']}")
            current_leader = leader_status['leader']
            if current_leader:
                print(f"    Leader: {current_leader[0]}:{current_leader[1]}")
            else:
                print(f"    Leader: None (election in progress)")
            print(f"    Eligible: {leader_status['eligible']}")
            print(f"    Fencing Token: {leader_status.get('fencing_token', 'N/A')}")
            print(f"    Lease Remaining: {leader_status['lease_remaining']:.1f}s")
            
            # Show known nodes
            print(f"\n  Known Nodes:")
            for node, state in server._incarnation_tracker.get_all_nodes():
                print(f"    {node[0]}:{node[1]} - Status: {state.status}, Inc: {state.incarnation}")
            
    except asyncio.CancelledError:
        print("\n[Server 1] Shutting down...")
        probe_task.cancel()
        election_task.cancel()
        try:
            await probe_task
        except asyncio.CancelledError:
            pass
        try:
            await election_task
        except asyncio.CancelledError:
            pass
        server.stop_probe_cycle()
        await server.stop_leader_election()
        await server.shutdown()
        print("[Server 1] Shutdown complete")


if __name__ == '__main__':
    try:
        asyncio.run(run_server_1())
    except KeyboardInterrupt:
        print("\n[Server 1] Interrupted by user")
