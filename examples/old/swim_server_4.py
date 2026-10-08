#!/usr/bin/env python3
"""
SWIM + Lifeguard Test Server 4 with Leadership Election (DC-WEST)

This server runs on ports 8676 (TCP) and 8677 (UDP) and demonstrates
the SWIM protocol with Lifeguard enhancements and hierarchical leadership.

Usage:
    python swim_server_4.py

This server will:
1. Start up and begin the probe cycle
2. Attempt to join servers 5 and 6 in DC-WEST
3. Also attempt cross-DC connection to DC-EAST (server 1)
4. Exchange probes and membership information
5. Participate in leader election
"""

import asyncio
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collections import defaultdict
from hyperscale.distributed.env import Env

# Import the SWIM server implementation from the swim package
from swim import UDPServer


async def run_server_4():
    """Run SWIM server 4 on ports 8676/8677 (DC-WEST)"""
    
    print("=" * 60)
    print("SWIM + Lifeguard + Leadership Test Server 4")
    print("=" * 60)
    print("TCP Port: 8676")
    print("UDP Port: 8677")
    print("Datacenter: DC-WEST")
    print("=" * 60)
    
    server = UDPServer(
        '127.0.0.1',
        8676,  # TCP port
        8677,  # UDP port
        Env(
            MERCURY_SYNC_REQUEST_TIMEOUT='2s',
        ),
        dc_id='DC-WEST',
        priority=1,  # High priority for leadership in DC-WEST
    )

    await server.start_server(init_context={
        'max_probe_timeout': 10,
        'min_probe_timeout': 1,
        'current_timeout': 2,
        'nodes': defaultdict(asyncio.Queue),
        'udp_poll_interval': 2,
        'suspicion_min_timeout': 2.0,
        'suspicion_max_timeout': 15.0,
    })
    
    print("\n[Server 4] Started successfully!")
    print(f"[Server 4] Node ID: {server.node_id}")
    print(f"[Server 4] Short ID: {server.node_id.short}")
    print(f"[Server 4] Local Health Multiplier: {server._local_health.get_multiplier():.2f}")
    print(f"[Server 4] Incarnation: {server.get_self_incarnation()}")
    
    # Wait a moment for other servers to potentially start
    await asyncio.sleep(2)
    
    # Try to join other DC-WEST servers
    dc_west_seeds = [
        ('127.0.0.1', 8679),  # Server 5
        ('127.0.0.1', 8681),  # Server 6
    ]
    
    for seed in dc_west_seeds:
        print(f"\n[Server 4] Attempting to join {seed}...")
        success = await server.join_cluster(seed, timeout=5.0)
        if success:
            print(f"[Server 4] Successfully joined cluster via {seed}")
        else:
            print(f"[Server 4] Failed to join {seed} (will retry via probe cycle)")
    
    # Also try cross-DC connection to DC-EAST
    dc_east_seed = ('127.0.0.1', 8671)  # Server 1 in DC-EAST
    print(f"\n[Server 4] Attempting cross-DC join to DC-EAST at {dc_east_seed}...")
    success = await server.join_cluster(dc_east_seed, timeout=5.0)
    if success:
        print("[Server 4] Successfully connected to DC-EAST")
    else:
        print("[Server 4] Failed to connect to DC-EAST (will retry)")
    
    # Start the probe cycle in the background
    print("\n[Server 4] Starting probe cycle...")
    probe_task = asyncio.create_task(server.start_probe_cycle())
    
    # Start leader election
    print("[Server 4] Starting leader election...")
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
        print("\n[Server 4] Shutting down...")
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
        print("[Server 4] Shutdown complete")


if __name__ == '__main__':
    try:
        asyncio.run(run_server_4())
    except KeyboardInterrupt:
        print("\n[Server 4] Interrupted by user")

