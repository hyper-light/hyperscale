#!/usr/bin/env python3
"""
Manager Test Server 4

This server runs on ports 9006 (TCP) and 9007 (UDP) and demonstrates
the Manager node with SWIM-based leader election and worker management.

Usage:
    python manager_4.py
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes import ManagerServer


async def run_manager_4():
    """Run Manager server 4 on ports 9006/9007"""
    
    print("=" * 60)
    print("Manager Test Server 4")
    print("=" * 60)
    print("TCP Port: 9006")
    print("UDP Port: 9007")
    print("Datacenter: DC-EAST")
    print("=" * 60)
    
    manager_peers_tcp = [
        ('127.0.0.1', 9000),  # Manager 1
        ('127.0.0.1', 9002),  # Manager 2
        ('127.0.0.1', 9004),  # Manager 3
        ('127.0.0.1', 9008),  # Manager 5
    ]
    manager_peers_udp = [
        ('127.0.0.1', 9001),  # Manager 1
        ('127.0.0.1', 9003),  # Manager 2
        ('127.0.0.1', 9005),  # Manager 3
        ('127.0.0.1', 9009),  # Manager 5
    ]
    
    gate_addrs_tcp = [
        ('127.0.0.1', 9100),
        ('127.0.0.1', 9102),
        ('127.0.0.1', 9104),
    ]
    gate_addrs_udp = [
        ('127.0.0.1', 9101),
        ('127.0.0.1', 9103),
        ('127.0.0.1', 9105),
    ]
    
    server = ManagerServer(
        host='127.0.0.1',
        tcp_port=9006,
        udp_port=9007,
        env=Env(MERCURY_SYNC_REQUEST_TIMEOUT='2s'),
        dc_id='DC-EAST',
        manager_peers=manager_peers_tcp,
        manager_udp_peers=manager_peers_udp,
        gate_addrs=gate_addrs_tcp,
        gate_udp_addrs=gate_addrs_udp,
    )
    
    await server.start()
    
    print(f"\n[Manager 4] Started! Node ID: {server._node_id.short}")
    
    try:
        while True:
            await asyncio.sleep(5)
            print(f"\n[Manager 4] State={server._manager_state.value}, Leader={server.is_leader()}, Workers={len(server._workers)}")
    except asyncio.CancelledError:
        await server.stop()


if __name__ == '__main__':
    try:
        asyncio.run(run_manager_4())
    except KeyboardInterrupt:
        print("\n[Manager 4] Interrupted")

