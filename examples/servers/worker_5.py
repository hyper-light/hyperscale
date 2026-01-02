#!/usr/bin/env python3
"""
Worker Test Server 5

This server runs on ports 9208 (TCP) and 9209 (UDP) and demonstrates
the Worker node with workflow execution capabilities.

Usage:
    python worker_5.py
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.distributed_rewrite.env import Env
from hyperscale.distributed_rewrite.nodes import WorkerServer


async def run_worker_5():
    """Run Worker server 5 on ports 9208/9209"""
    
    print("=" * 60)
    print("Worker Test Server 5")
    print("=" * 60)
    print("TCP Port: 9208")
    print("UDP Port: 9209")
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
        tcp_port=9208,
        udp_port=9209,
        env=Env(MERCURY_SYNC_REQUEST_TIMEOUT='2s'),
        dc_id='DC-EAST',
        total_cores=4,
        seed_managers=seed_managers,
    )
    
    await server.start()
    
    print(f"\n[Worker 5] Started! Node ID: {server._node_id.short}, Cores: {server._total_cores}")
    
    try:
        while True:
            await asyncio.sleep(5)
            print(f"\n[Worker 5] Cores={server._available_cores}/{server._total_cores}, Workflows={len(server._workflows)}, Managers={len(server._known_managers)}")
    except asyncio.CancelledError:
        await server.stop()


if __name__ == '__main__':
    try:
        asyncio.run(run_worker_5())
    except KeyboardInterrupt:
        print("\n[Worker 5] Interrupted")

