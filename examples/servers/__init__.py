"""
Test Servers for Hyperscale Distributed System

This package contains test server scripts for each node type:
- Managers (manager_1.py - manager_5.py)
- Gates (gate_1.py - gate_5.py)
- Workers (worker_1.py - worker_5.py)

Port Allocation:
    Managers: TCP 9000-9008 (even), UDP 9001-9009 (odd)
    Gates:    TCP 9100-9108 (even), UDP 9101-9109 (odd)
    Workers:  TCP 9200-9208 (even), UDP 9201-9209 (odd)

Usage:
    # Start managers first (they form the quorum cluster)
    python -m examples.servers.manager_1
    python -m examples.servers.manager_2
    python -m examples.servers.manager_3
    
    # Start workers (they register with managers)
    python -m examples.servers.worker_1
    python -m examples.servers.worker_2
    
    # Optionally start gates (for multi-DC routing)
    python -m examples.servers.gate_1
    python -m examples.servers.gate_2
"""

