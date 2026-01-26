#!/usr/bin/env python3
"""
Gate Test Server 5

This server runs on ports 9108 (TCP) and 9109 (UDP) and demonstrates
the Gate node with SWIM-based leader election and job routing.

Usage:
    python gate_5.py
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes import GateServer


async def run_gate_5():
    """Run Gate server 5 on ports 9108/9109"""
    
    print("=" * 60)
    print("Gate Test Server 5")
    print("=" * 60)
    print("TCP Port: 9108")
    print("UDP Port: 9109")
    print("=" * 60)
    
    gate_peers_tcp = [
        ('127.0.0.1', 9100),  # Gate 1
        ('127.0.0.1', 9102),  # Gate 2
        ('127.0.0.1', 9104),  # Gate 3
        ('127.0.0.1', 9106),  # Gate 4
    ]
    gate_peers_udp = [
        ('127.0.0.1', 9101),  # Gate 1
        ('127.0.0.1', 9103),  # Gate 2
        ('127.0.0.1', 9105),  # Gate 3
        ('127.0.0.1', 9107),  # Gate 4
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
        tcp_port=9108,
        udp_port=9109,
        env=Env(MERCURY_SYNC_REQUEST_TIMEOUT='2s'),
        dc_id='global',
        gate_peers=gate_peers_tcp,
        gate_udp_peers=gate_peers_udp,
        datacenter_managers=datacenter_managers_tcp,
        datacenter_manager_udp=datacenter_managers_udp,
    )
    
    await server.start()
    
    print(f"\n[Gate 5] Started! Node ID: {server._node_id.short}")
    
    try:
        while True:
            await asyncio.sleep(5)
            print(f"\n[Gate 5] State={server._gate_state.value}, Leader={server.is_leader()}, Jobs={len(server._jobs)}")
    except asyncio.CancelledError:
        await server.stop()


if __name__ == '__main__':
    try:
        asyncio.run(run_gate_5())
    except KeyboardInterrupt:
        print("\n[Gate 5] Interrupted")

