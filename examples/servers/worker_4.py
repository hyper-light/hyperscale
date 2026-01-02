#!/usr/bin/env python3
"""
Worker Test Server 4

This server runs on ports 9206 (TCP) and 9207 (UDP) and demonstrates
the Worker node with workflow execution capabilities.

Usage:
    python worker_4.py
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.distributed_rewrite.env import Env
from hyperscale.distributed_rewrite.nodes import WorkerServer


async def run_worker_4():
    """Run Worker server 4 on ports 9206/9207"""
    
    print("=" * 60)
    print("Worker Test Server 4")
    print("=" * 60)
    print("TCP Port: 9206")
    print("UDP Port: 9207")
    print("Datacenter: DC-EAST")
    print("=" * 60)
    
    seed_managers = [
        ('127.0.0.1', 9000),
        ('127.0.0.1', 9002),
        ('127.0.0.1', 9004),
        ('127.0.0.1', 9006),
        ('127.0.0.1', 9008),
    ]
    
    server = WorkerServer(
        host='127.0.0.1',
        tcp_port=9206,
        udp_port=9207,
        env=Env(MERCURY_SYNC_REQUEST_TIMEOUT='2s'),
        dc_id='DC-EAST',
        total_cores=4,
        seed_managers=seed_managers,
    )
    
    await server.start()
    
    print(f"\n[Worker 4] Started! Node ID: {server._node_id.short}, Cores: {server._total_cores}")
    
    try:
        while True:
            await asyncio.sleep(5)
            print(f"\n[Worker 4] Cores={server._available_cores}/{server._total_cores}, Workflows={len(server._workflows)}, Managers={len(server._known_managers)}")
    except asyncio.CancelledError:
        await server.stop()


if __name__ == '__main__':
    try:
        asyncio.run(run_worker_4())
    except KeyboardInterrupt:
        print("\n[Worker 4] Interrupted")

