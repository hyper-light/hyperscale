#!/usr/bin/env python3
"""
SWIM + Lifeguard Test Server 3 with Leadership Election

This server runs on ports 8674 (TCP) and 8675 (UDP) and completes
the 3-node cluster for proper quorum-based leader election.

With 3 nodes, majority quorum is 2 votes, which prevents split-brain
scenarios that occur with only 2 nodes.

Usage:
    python swim_server_3.py

This server will:
1. Start up and begin the probe cycle
2. Join the existing cluster (server 1 and 2)
3. Exchange probes and membership information
4. Participate in leader election (with proper quorum)
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
from swim import TestServer


async def run_server_3():
    """Run SWIM server 3 on ports 8674/8675"""
    
    print("=" * 60)
    print("SWIM + Lifeguard + Leadership Test Server 3")
    print("=" * 60)
    print("TCP Port: 8674")
    print("UDP Port: 8675")
    print("Datacenter: DC-EAST")
    print("=" * 60)
    
    server = TestServer(
        '127.0.0.1',
        8674,  # TCP port
        8675,  # UDP port
        Env(
            MERCURY_SYNC_REQUEST_TIMEOUT='2s',
        ),
        dc_id='DC-EAST',
        priority=3,  # Lower priority for leadership
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
    
    print("\n[Server 3] Started successfully!")
    print(f"[Server 3] Node ID: {server.node_id}")
    print(f"[Server 3] Short ID: {server.node_id.short}")
    print(f"[Server 3] Local Health Multiplier: {server._local_health.get_multiplier():.2f}")
    print(f"[Server 3] Incarnation: {server.get_self_incarnation()}")
    
    # Wait a moment for other servers to potentially start
    await asyncio.sleep(3)
    
    # Join both server 1 and server 2
    servers_to_join = [
        ('127.0.0.1', 8671),  # Server 1
        ('127.0.0.1', 8673),  # Server 2
    ]
    
    # Our address to advertise
    self_udp_addr = ('127.0.0.1', 8675)
    
    for server_addr in servers_to_join:
        print(f"\n[Server 3] Attempting to join server at {server_addr}...")
        
        try:
            # Send join message with OUR address (the node joining)
            join_msg = b'join>' + f'{self_udp_addr[0]}:{self_udp_addr[1]}'.encode()
            server._task_runner.run(
                server.send,
                server_addr,
                join_msg,
                timeout=5,
            )
            print(f"[Server 3] Join request sent to {server_addr}")
            
            # Add to our known nodes
            nodes = server._context.read('nodes')
            nodes[server_addr].put_nowait((0, b'OK'))
            server._probe_scheduler.add_member(server_addr)
            server._incarnation_tracker.update_node(
                server_addr, b'OK', 0, 0
            )
            print(f"[Server 3] Added {server_addr} to membership list")
            
        except Exception as e:
            print(f"[Server 3] Failed to join {server_addr}: {e}")
    
    # Start the probe cycle in the background
    print("\n[Server 3] Starting probe cycle...")
    probe_task = asyncio.create_task(server.start_probe_cycle())
    
    # Wait a bit for membership to stabilize before starting election
    await asyncio.sleep(2)
    
    # Start leader election
    print("[Server 3] Starting leader election...")
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
        print("\n[Server 3] Shutting down...")
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
        print("[Server 3] Shutdown complete")


if __name__ == '__main__':
    try:
        asyncio.run(run_server_3())
    except KeyboardInterrupt:
        print("\n[Server 3] Interrupted by user")

