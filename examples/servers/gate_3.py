#!/usr/bin/env python3
"""
Gate Test Server 3

This server runs on ports 9104 (TCP) and 9105 (UDP) and demonstrates
the Gate node with SWIM-based leader election and job routing.

Usage:
    python gate_3.py
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.distributed_rewrite.env import Env
from hyperscale.distributed_rewrite.nodes import GateServer


async def run_gate_3():
    """Run Gate server 3 on ports 9104/9105"""
    
    print("=" * 60)
    print("Gate Test Server 3")
    print("=" * 60)
    print("TCP Port: 9104")
    print("UDP Port: 9105")
    print("=" * 60)
    
    gate_peers_tcp = [
        ('127.0.0.1', 9100),  # Gate 1
        ('127.0.0.1', 9102),  # Gate 2
        ('127.0.0.1', 9106),  # Gate 4
        ('127.0.0.1', 9108),  # Gate 5
    ]
    gate_peers_udp = [
        ('127.0.0.1', 9101),  # Gate 1
        ('127.0.0.1', 9103),  # Gate 2
        ('127.0.0.1', 9107),  # Gate 4
        ('127.0.0.1', 9109),  # Gate 5
    ]
    
    datacenter_managers_tcp = {
        'DC-EAST': [
            ('127.0.0.1', 9000),
            ('127.0.0.1', 9002),
            ('127.0.0.1', 9004),
            ('127.0.0.1', 9006),
            ('127.0.0.1', 9008),
        ],
    }
    datacenter_managers_udp = {
        'DC-EAST': [
            ('127.0.0.1', 9001),
            ('127.0.0.1', 9003),
            ('127.0.0.1', 9005),
            ('127.0.0.1', 9007),
            ('127.0.0.1', 9009),
        ],
    }
    
    server = GateServer(
        host='127.0.0.1',
        tcp_port=9104,
        udp_port=9105,
        env=Env(MERCURY_SYNC_REQUEST_TIMEOUT='2s'),
        dc_id='global',
        gate_peers=gate_peers_tcp,
        gate_udp_peers=gate_peers_udp,
        datacenter_managers=datacenter_managers_tcp,
        datacenter_manager_udp=datacenter_managers_udp,
    )
    
    await server.start()
    
    print(f"\n[Gate 3] Started! Node ID: {server._node_id.short}")
    
    try:
        while True:
            await asyncio.sleep(5)
            print(f"\n[Gate 3] State={server._gate_state.value}, Leader={server.is_leader()}, Jobs={len(server._jobs)}")
    except asyncio.CancelledError:
        await server.stop()


if __name__ == '__main__':
    try:
        asyncio.run(run_gate_3())
    except KeyboardInterrupt:
        print("\n[Gate 3] Interrupted")

