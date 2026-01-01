#!/usr/bin/env python3
"""
SWIM + Lifeguard Test Runner

This script launches both SWIM test servers and demonstrates the
Lifeguard enhancements for failure detection.

Usage:
    python swim_test_runner.py

The test will:
1. Start Server 1 and Server 2
2. They will discover each other and exchange membership
3. Probe each other using SWIM protocol
4. If one server is killed, the other will:
   - Attempt direct probe
   - Try indirect probe via other nodes (if available)
   - Start suspicion timer
   - Eventually declare the node dead

Lifeguard Enhancements Demonstrated:
- Local Health Multiplier (LHM) affects probe timeouts
- Suspicion confirmations reduce timeout
- Refutation with incarnation number increment
- Piggybacked gossip for efficient dissemination
"""

import asyncio
import subprocess
import sys
import os
import signal
import time


def run_test():
    """Run both SWIM servers and monitor their interaction."""
    
    print("=" * 70)
    print("SWIM + Lifeguard Protocol Test")
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
    print()
    print("=" * 70)
    print()
    
    # Get the directory containing this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Start Server 1
    print("[Runner] Starting Server 1...")
    server1_proc = subprocess.Popen(
        [sys.executable, os.path.join(script_dir, 'swim_server_1.py')],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    
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
    
    print("[Runner] Both servers started. Press Ctrl+C to stop.")
    print()
    print("-" * 70)
    print("Server Output:")
    print("-" * 70)
    print()
    
    try:
        # Read and display output from both servers
        import select
        
        while True:
            # Check if processes are still running
            if server1_proc.poll() is not None and server2_proc.poll() is not None:
                break
            
            # Read available output
            for proc, name in [(server1_proc, 'S1'), (server2_proc, 'S2')]:
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
        
        # Send SIGINT to both processes
        try:
            server1_proc.send_signal(signal.SIGINT)
        except Exception:
            pass
        
        try:
            server2_proc.send_signal(signal.SIGINT)
        except Exception:
            pass
        
        # Wait for graceful shutdown
        time.sleep(2)
        
        # Force kill if still running
        try:
            server1_proc.terminate()
        except Exception:
            pass
        
        try:
            server2_proc.terminate()
        except Exception:
            pass
        
        print("[Runner] Servers stopped.")
    
    print()
    print("=" * 70)
    print("Test Complete")
    print("=" * 70)


if __name__ == '__main__':
    run_test()

