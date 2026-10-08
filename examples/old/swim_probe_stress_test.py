#!/usr/bin/env python3
"""
Stress test for the ProbeScheduler's lockless copy-on-write implementation.

Tests concurrent reads (get_next_target) with writes (update_members, add_member, remove_member)
to verify the lockless pattern works correctly under load.
"""

import asyncio
import random
import time
from examples.swim.detection.probe_scheduler import ProbeScheduler


async def stress_test_probe_scheduler():
    """Run stress test simulating concurrent probe cycle and membership changes."""
    
    print("=" * 60)
    print("ProbeScheduler Lockless Copy-on-Write Stress Test")
    print("=" * 60)
    
    scheduler = ProbeScheduler(protocol_period=0.1)
    
    # Initial members
    initial_members = [(f"10.0.0.{i}", 8000 + i) for i in range(1, 11)]
    scheduler.update_members(initial_members)
    
    print(f"\nInitial members: {len(scheduler.members)}")
    print(f"Initial stats: {scheduler.get_stats()}")
    
    # Counters for verification
    probes_done = 0
    members_added = 0
    members_removed = 0
    updates_done = 0
    errors = []
    
    # Flag to stop workers
    running = True
    
    async def probe_worker(worker_id: int):
        """Simulates rapid probe cycles - reads from the scheduler."""
        nonlocal probes_done
        
        while running:
            try:
                target = scheduler.get_next_target()
                if target:
                    probes_done += 1
                await asyncio.sleep(0.001)  # 1ms between probes (1000 probes/sec per worker)
            except Exception as e:
                errors.append(f"Probe worker {worker_id}: {type(e).__name__}: {e}")
    
    async def membership_changer():
        """Simulates membership changes - writes to the scheduler."""
        nonlocal members_added, members_removed, updates_done
        
        member_pool = [(f"10.0.1.{i}", 9000 + i) for i in range(1, 101)]
        
        while running:
            try:
                action = random.choice(['add', 'remove', 'update'])
                
                if action == 'add' and len(scheduler.members) < 50:
                    new_member = random.choice(member_pool)
                    if new_member not in scheduler._member_set:
                        scheduler.add_member(new_member)
                        members_added += 1
                
                elif action == 'remove' and len(scheduler.members) > 5:
                    if scheduler.members:
                        old_member = random.choice(scheduler.members)
                        scheduler.remove_member(old_member)
                        members_removed += 1
                
                elif action == 'update':
                    # Random subset of pool
                    new_members = random.sample(member_pool, random.randint(5, 20))
                    scheduler.update_members(new_members)
                    updates_done += 1
                
                await asyncio.sleep(0.01)  # 100 changes/sec
            except Exception as e:
                errors.append(f"Membership changer: {type(e).__name__}: {e}")
    
    async def stats_reporter():
        """Periodically report stats."""
        while running:
            await asyncio.sleep(1.0)
            stats = scheduler.get_stats()
            print(f"  Members: {stats['member_count']:3d} | "
                  f"Probes: {probes_done:6d} | "
                  f"Adds: {members_added:4d} | "
                  f"Removes: {members_removed:4d} | "
                  f"Updates: {updates_done:4d} | "
                  f"Cycles: {stats['cycles_completed']:4d}")
    
    # Start workers
    print("\nStarting stress test (5 seconds)...")
    print("-" * 60)
    
    start_time = time.monotonic()
    
    # Create 10 probe workers + 1 membership changer + 1 stats reporter
    tasks = [
        asyncio.create_task(probe_worker(i)) for i in range(10)
    ]
    tasks.append(asyncio.create_task(membership_changer()))
    tasks.append(asyncio.create_task(stats_reporter()))
    
    # Run for 5 seconds
    await asyncio.sleep(5.0)
    running = False
    
    # Wait for workers to finish
    for task in tasks:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    
    elapsed = time.monotonic() - start_time
    
    print("-" * 60)
    print("\nResults:")
    print(f"  Duration: {elapsed:.2f}s")
    print(f"  Total probes: {probes_done:,}")
    print(f"  Probes/sec: {probes_done / elapsed:,.0f}")
    print(f"  Members added: {members_added}")
    print(f"  Members removed: {members_removed}")
    print(f"  Full updates: {updates_done}")
    print(f"  Final member count: {len(scheduler.members)}")
    print(f"  Final stats: {scheduler.get_stats()}")
    
    if errors:
        print(f"\n❌ ERRORS ({len(errors)}):")
        for err in errors[:10]:  # Show first 10
            print(f"    {err}")
        return False
    else:
        print("\n✅ No errors! Lockless pattern working correctly.")
        return True


async def test_correctness():
    """Test that the lockless pattern maintains correctness."""
    
    print("\n" + "=" * 60)
    print("Correctness Tests")
    print("=" * 60)
    
    scheduler = ProbeScheduler()
    
    # Test 1: Empty scheduler
    print("\n1. Empty scheduler returns None")
    assert scheduler.get_next_target() is None
    print("   ✅ Pass")
    
    # Test 2: Add members
    print("\n2. Add members")
    scheduler.add_member(("10.0.0.1", 8001))
    scheduler.add_member(("10.0.0.2", 8002))
    scheduler.add_member(("10.0.0.3", 8003))
    assert len(scheduler.members) == 3
    print(f"   ✅ Pass - {len(scheduler.members)} members")
    
    # Test 3: Get targets cycles through all members
    print("\n3. Round-robin cycling")
    seen = set()
    for _ in range(10):
        target = scheduler.get_next_target()
        seen.add(target)
    assert len(seen) == 3  # Should have seen all 3 members
    print(f"   ✅ Pass - saw all {len(seen)} members")
    
    # Test 4: Remove member
    print("\n4. Remove member")
    scheduler.remove_member(("10.0.0.2", 8002))
    assert len(scheduler.members) == 2
    assert ("10.0.0.2", 8002) not in scheduler._member_set
    print(f"   ✅ Pass - {len(scheduler.members)} members remain")
    
    # Test 5: Update members (full replacement)
    print("\n5. Update members (full replacement)")
    new_members = [(f"192.168.1.{i}", 9000 + i) for i in range(5)]
    scheduler.update_members(new_members)
    assert len(scheduler.members) == 5
    print(f"   ✅ Pass - {len(scheduler.members)} new members")
    
    # Test 6: Duplicate add is no-op
    print("\n6. Duplicate add is no-op")
    scheduler.add_member(("192.168.1.1", 9001))
    assert len(scheduler.members) == 5
    print("   ✅ Pass - count unchanged")
    
    # Test 7: Remove non-existent is no-op
    print("\n7. Remove non-existent is no-op")
    scheduler.remove_member(("10.0.0.99", 9999))
    assert len(scheduler.members) == 5
    print("   ✅ Pass - count unchanged")
    
    print("\n" + "=" * 60)
    print("All correctness tests passed! ✅")
    print("=" * 60)
    return True


async def main():
    """Run all tests."""
    
    correctness_ok = await test_correctness()
    stress_ok = await stress_test_probe_scheduler()
    
    print("\n" + "=" * 60)
    if correctness_ok and stress_ok:
        print("ALL TESTS PASSED ✅")
    else:
        print("SOME TESTS FAILED ❌")
    print("=" * 60)
    
    return correctness_ok and stress_ok


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)

