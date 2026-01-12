---
Summary Table
| # | Severity | Issue | File:Line | Type |
|---|----------|-------|-----------|------|
| 1 | 🔴 CRITICAL | fsync parameter doesn't exist | job_ledger.py:217,259,302,343 | API Mismatch |
| 2 | 🔴 CRITICAL | Unbounded queue | wal_writer.py:77 | Memory Leak |
| 3 | 🔴 CRITICAL | Exception swallowing | wal_writer.py:191-192 | Policy Violation |
| 4 | 🟠 HIGH | Futures hang when loop=None | wal_writer.py:302-309 | Deadlock |
| 5 | 🟠 HIGH | Cache not thread-safe | bounded_lru_cache.py:27-36 | Race Condition |
| 6 | 🟠 HIGH | Snapshot copy on every op | node_wal.py, job_ledger.py | Memory Leak |
| 7 | 🟠 HIGH | Executor not shutdown | wal_writer.py:106-122 | Resource Leak |
| 8 | 🟠 HIGH | Checkpoint save race | checkpoint.py:112-129 | Race Condition |
| 9 | 🟠 HIGH | Snapshot read without lock | job_ledger.py:365-374 | Race Condition |
| 10 | 🟡 MEDIUM | Missing terminal check | job_ledger.py:274-288 | Logic Error |
| 11 | 🟡 MEDIUM | Invalid state transitions | job_state.py | State Machine |
| 12 | 🟡 MEDIUM | Silent transition failures | node_wal.py:212-246 | Silent Failure |
| 13 | 🟡 MEDIUM | REGIONAL state skipped | node_wal.py:228 | State Machine |
| 14 | 🟡 MEDIUM | No queue bounds | wal_writer.py:77 | Backpressure |
| 15 | 🟡 MEDIUM | No QueueFull handling | distributed/ledger/ | Backpressure |
| 16 | 🟡 MEDIUM | No tier flow control | distributed/ledger/ | Backpressure |
| 17 | 🟡 MEDIUM | Timeout cleanup | commit_pipeline.py:142-158 | Orphaned State |
---