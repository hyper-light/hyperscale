#!/usr/bin/env python3
"""
SWIM + Lifeguard Test Runner with 3-Node Quorum

This script launches three SWIM test servers and demonstrates the
Lifeguard enhancements for failure detection AND proper leader election
with majority quorum.

Usage:
    python swim_test_runner.py

The test will:
1. Start Server 1, Server 2, and Server 3
2. They will discover each other and exchange membership
3. Probe each other using SWIM protocol
4. Participate in leader election with proper 3-node quorum
5. If one server is killed, the other two will:
   - Attempt direct probe
   - Try indirect probe via other nodes
   - Start suspicion timer
   - Eventually declare the node dead
   - Re-elect a leader if necessary

Lifeguard Enhancements Demonstrated:
- Local Health Multiplier (LHM) affects probe timeouts
- Suspicion confirmations reduce timeout
- Refutation with incarnation number increment
- Piggybacked gossip for efficient dissemination

Split-Brain Prevention Demonstrated:
- Pre-voting prevents unnecessary elections
- Term-based auto-resolution when leaders discover each other
- Fencing tokens for operation safety
- 3-node quorum ensures single leader
"""

import asyncio
import subprocess
import sys
import os
import signal
import time


def run_test():
    """Run three SWIM servers and monitor their interaction."""
    
    print("=" * 70)
    print("SWIM + Lifeguard Protocol Test with 3-Node Quorum")
    print("=" * 70)
    print()
    print("This test demonstrates:")
    print("  1. Local Health Multiplier (LHM) for adaptive timeouts")
    print("  2. Incarnation numbers for message ordering")
    print("  3. Suspicion timer with confirmation-based timeout")
    print("  4. Indirect probing via proxy nodes")
    print("  5. Refutation broadcast with incarnation increment")
    print("  6. Piggybacked gossip for efficient dissemination")
    print("  7. Round-robin probe scheduling")
    print("  8. Pre-voting for split-brain prevention")
    print("  9. 3-node quorum for proper leader election")
    print(" 10. Fencing tokens for operation safety")
    print()
    print("=" * 70)
    print()
    
    # Get the directory containing this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    procs = []
    
    # Start Server 1
    print("[Runner] Starting Server 1...")
    server1_proc = subprocess.Popen(
        [sys.executable, os.path.join(script_dir, 'swim_server_1.py')],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    procs.append((server1_proc, 'S1'))
    
    # Wait a moment for server 1 to start
    time.sleep(1)
    
    # Start Server 2
    print("[Runner] Starting Server 2...")
    server2_proc = subprocess.Popen(
        [sys.executable, os.path.join(script_dir, 'swim_server_2.py')],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    procs.append((server2_proc, 'S2'))
    
    # Wait a moment for server 2 to start
    time.sleep(1)
    
    # Start Server 3
    print("[Runner] Starting Server 3...")
    server3_proc = subprocess.Popen(
        [sys.executable, os.path.join(script_dir, 'swim_server_3.py')],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    procs.append((server3_proc, 'S3'))
    
    print("[Runner] All three servers started. Press Ctrl+C to stop.")
    print()
    print("-" * 70)
    print("Server Output:")
    print("-" * 70)
    print()
    
    try:
        # Read and display output from all servers
        while True:
            # Check if all processes have finished
            if all(proc.poll() is not None for proc, _ in procs):
                break
            
            # Read available output from each process
            for proc, name in procs:
                if proc.stdout:
                    try:
                        line = proc.stdout.readline()
                        if line:
                            print(f"[{name}] {line.rstrip()}")
                    except Exception:
                        pass
            
            time.sleep(0.1)
            
    except KeyboardInterrupt:
        print()
        print("-" * 70)
        print("[Runner] Stopping servers...")
        
        # Send SIGINT to all processes
        for proc, name in procs:
            try:
                proc.send_signal(signal.SIGINT)
            except Exception:
                pass
        
        # Wait for graceful shutdown
        time.sleep(2)
        
        # Force kill if still running
        for proc, name in procs:
            try:
                proc.terminate()
            except Exception:
                pass
        
        print("[Runner] Servers stopped.")
    
    print()
    print("=" * 70)
    print("Test Complete")
    print("=" * 70)


if __name__ == '__main__':
    run_test()

