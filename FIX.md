# AD-26 and AD-28 Compliance Fixes

This document lists the **exact changes** required to reach compliance.

## AD-26 (Adaptive Healthcheck Extensions)

### 1) Fix the heartbeat piggyback request payload
**Problem**: `HealthcheckExtensionRequest` requires `estimated_completion` and `active_workflow_count`, but the heartbeat path constructs it without those fields.

**Change**:
- In `hyperscale/distributed_rewrite/nodes/manager.py` (heartbeat piggyback handler), populate **all required fields** when creating `HealthcheckExtensionRequest`:
  - `worker_id`
  - `progress`
  - `estimated_completion`
  - `active_workflow_count`

**Acceptance**:
- No `TypeError` on construction.
- Manager receives a well-formed extension request from heartbeat path.

---

### 2) Fix worker extension progress semantics
**Problem**: `Worker.request_extension()` clamps progress to `0..1`, which prevents the “must strictly increase” rule from working for long-running jobs.

**Change**:
- In `hyperscale/distributed_rewrite/nodes/worker.py`, stop clamping progress to `0..1`.
- Use a **monotonic per-workflow progress value** (e.g., `completed_count + failed_count`, or per-workflow sequence) so successive extension requests always increase when real work advances.

**Acceptance**:
- ExtensionTracker grants can proceed as long as work advances.
- No false denials once progress exceeds 1.0.

---

### 3) Wire deadline enforcement to actual decisions
**Problem**: Deadlines are tracked, but enforcement is not consistently connected to eviction/timeout decisions.

**Change**:
- Ensure the **deadline enforcement loop** drives the same state transitions as other failure paths:
  - On grace expiry, trigger job-layer suspicion or eviction pathways consistently.
  - Ensure this path is logged and metrics are emitted.

**Acceptance**:
- Missed deadline → deterministic suspicion/eviction within configured bounds.

---

## AD-28 (Discovery + Secure Registration)

### 1) Enforce role-based mTLS validation
**Problem**: `RoleValidator` exists but is unused; `extract_claims_from_cert` is stubbed.

**Change**:
- Implement `extract_claims_from_cert` and use it in `RoleValidator.validate_connection()`.
- Call `RoleValidator` in **all** discovery registration / connection paths before accepting peer info.

**Acceptance**:
- Connections without valid role claims are rejected.

---

### 2) Add cluster/environment IDs to wire protocol and enforce
**Problem**: `cluster_id`/`environment_id` are required by AD-28 but are not in wire models.

**Change**:
- Add `cluster_id` and `environment_id` fields to all relevant registration dataclasses in `hyperscale/distributed_rewrite/models/distributed.py`.
- Validate these fields **before** processing any other data in registration handlers.

**Acceptance**:
- Any mismatch rejects the connection (with logs/metrics).

---

### 3) Implement real DNS SRV lookup
**Problem**: DNS resolver only parses `hostname:port` strings; AD-28 requires real SRV support.

**Change**:
- Implement SRV resolution in `hyperscale/distributed_rewrite/discovery/dns/resolver.py` and use it when configured.
- Preserve existing hostname:port fallback behavior.

**Acceptance**:
- SRV records are resolved to targets/ports at runtime.

---

### 4) Integrate connection pooling/stickiness into DiscoveryService
**Problem**: `ConnectionPool`/`StickyConnection` exist but are not used by `DiscoveryService`.

**Change**:
- Wire `ConnectionPool` into `hyperscale/distributed_rewrite/discovery/discovery_service.py` for selection and reuse.
- Use sticky behavior for “sessioned” requests where affinity matters (per AD-28).

**Acceptance**:
- Discovery uses pooled/sticky connections instead of new connections each time.

---

## Deliverable Checklist

- [ ] Heartbeat extension payload includes required fields
- [ ] Worker extension progress is monotonic (no clamp)
- [ ] Deadline enforcement tied to eviction/suspicion
- [ ] RoleValidator is real and enforced
- [ ] `cluster_id`/`environment_id` added + validated
- [ ] Real SRV lookup implemented
- [ ] ConnectionPool integrated into DiscoveryService
