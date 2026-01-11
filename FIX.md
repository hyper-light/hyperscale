# Hardening Items (Non-blocking)

## 1) Job stats aggregation for completed_count
**Problem**: `JobInfo.completed_count` is still TODO and doesn’t aggregate from sub‑workflows.

**Exact changes**:
- Implement aggregation of completed sub‑workflows into `completed_count` during job updates.

**References**:
- `hyperscale/distributed_rewrite/models/jobs.py:344`

---

## 2) Make timeout check interval configurable
**Problem**: Manager timeout loop uses hardcoded `check_interval = 30.0`.

**Exact changes**:
- Add `JOB_TIMEOUT_CHECK_INTERVAL` to `env.py` and use it in `_unified_timeout_loop()`.

**References**:
- `hyperscale/distributed_rewrite/nodes/manager_impl.py:9377`
- `hyperscale/distributed_rewrite/env/env.py:146`
