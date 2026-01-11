#!/usr/bin/env python3
"""
Worker Test Server 2

This server runs on ports 9202 (TCP) and 9203 (UDP) and demonstrates
the Worker node with workflow execution capabilities.

Usage:
    python worker_2.py
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes import WorkerServer


async def run_worker_2():
    """Run Worker server 2 on ports 9202/9203"""
    
    print("=" * 60)
    print("Worker Test Server 2")
    print("=" * 60)
    print("TCP Port: 9202")
    print("UDP Port: 9203")
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
        tcp_port=9202,
        udp_port=9203,
        env=Env(MERCURY_SYNC_REQUEST_TIMEOUT='2s'),
        dc_id='DC-EAST',
        total_cores=4,
        seed_managers=seed_managers,
    )
    
    await server.start()
    
    print(f"\n[Worker 2] Started! Node ID: {server._node_id.short}, Cores: {server._total_cores}")
    
    try:
        while True:
            await asyncio.sleep(5)
            print(f"\n[Worker 2] Cores={server._available_cores}/{server._total_cores}, Workflows={len(server._workflows)}, Managers={len(server._known_managers)}")
    except asyncio.CancelledError:
        await server.stop()


if __name__ == '__main__':
    try:
        asyncio.run(run_worker_2())
    except KeyboardInterrupt:
        print("\n[Worker 2] Interrupted")

