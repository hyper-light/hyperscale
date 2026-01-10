# AD-35: Vivaldi Network Coordinates with Role-Aware Failure Detection

**Status**: Proposed
**Related**: AD-29 (Peer Confirmation), AD-30 (Hierarchical Failure Detection), AD-33 (Federated Health Monitoring)

---

## Problem Statement

The current failure detection system has three critical gaps for globally-distributed, multi-tier architectures:

### 1. **Geographic Latency Blindness**
Gates detecting managers across datacenters use **static timeouts** that don't account for network distance:
- Same-region manager (10ms RTT): 30s timeout is too conservative
- Cross-continent manager (150ms RTT): 30s timeout causes false positives
- Intercontinental manager (300ms RTT): 30s timeout is dangerously aggressive

**Result**: False positives from geographic latency variance, or overly conservative timeouts that delay failure detection.

### 2. **Role-Agnostic Confirmation Strategy**
All peers are treated identically during unconfirmed peer cleanup (AD-29):
- **Gates** (cross-DC, high-latency): Need proactive confirmation with retries
- **Managers** (moderate load): Need load-aware confirmation
- **Workers** (extreme load): Probing stressed workers adds MORE load

**Result**: Either we're too aggressive (removing legitimate slow peers) or too passive (accumulating memory from dead peers).

### 3. **No Network Topology Learning**
The system cannot learn or adapt to actual network conditions:
- Static datacenter configuration required
- No adaptation to route changes, CDN shifts, or network degradation
- Cannot predict RTT to peers without direct measurement

**Result**: Manual tuning required for each deployment topology, and no automatic adaptation to changing conditions.

---

## Solution: Vivaldi Coordinates + Role-Aware Detection + Lifecycle States

Combine three architectural improvements:

1. **Vivaldi Network Coordinates**: Learn network topology and predict RTT
2. **Role-Aware Confirmation Strategies**: Tailor timeout/confirmation logic to peer role (Gate/Manager/Worker)
3. **UNCONFIRMED Lifecycle State**: Explicit state for unconfirmed peers (from AD-29 analysis)

---

## Part 1: Vivaldi Network Coordinates

### What is Vivaldi?

Vivaldi is a **decentralized network coordinate system** where each node maintains a position in a virtual coordinate space. The distance between two nodes in this space approximates their network RTT.

**Key Properties**:
- ✅ **Decentralized**: Each node calculates its own coordinates independently
- ✅ **Adaptive**: Coordinates converge as network conditions change
- ✅ **Predictive**: Estimate RTT to nodes without direct measurement
- ✅ **Low overhead**: Coordinates are small (~50 bytes) and piggyback on existing messages

### How It Works

Each node maintains a **VivaldiCoordinate**:
```python
@dataclass
class VivaldiCoordinate:
    position: list[float]  # N-dimensional coordinate (typically 4D)
    height: float          # Models asymmetric routes
    error: float          # Prediction confidence (lower = better)
```

**Update Algorithm** (simplified):
1. Node A sends ping to Node B with A's coordinate
2. Node B responds with ack, B's coordinate, and measured RTT
3. Node A updates its position to reduce prediction error:
   ```
   predicted_rtt = distance(A.coord, B.coord)
   error = measured_rtt - predicted_rtt
   A.position += delta * error * unit_vector(B.coord → A.coord)
   ```

**Convergence**: Typically 10-20 measurement rounds (~10-20 seconds with 1s probe interval).

### Integration with SWIM

Vivaldi coordinates **piggyback on existing SWIM messages** with zero additional probes:

```python
# Ping message (already exists in SWIM)
{
    "type": "ping",
    "from": ("10.0.1.5", 8000),
    "seq": 42,
    "vivaldi_coord": {  # NEW: Add coordinate (50 bytes)
        "position": [1.2, -0.5, 3.1, 0.8],
        "height": 0.3,
        "error": 0.15,
    },
}

# Ack message (already exists in SWIM)
{
    "type": "ack",
    "from": ("10.0.2.7", 8000),
    "seq": 42,
    "rtt_ms": 145.3,  # Measured RTT
    "vivaldi_coord": {  # NEW: Add coordinate (50 bytes)
        "position": [5.1, 2.3, -1.2, 0.4],
        "height": 0.5,
        "error": 0.22,
    },
}
```

**Total overhead**: ~50-80 bytes per message (negligible compared to existing SWIM gossip).

---

## Part 2: Role-Aware Failure Detection

### Peer Roles

Classify peers into three roles based on their position in the architecture:

```python
class PeerRole(Enum):
    GATE = "gate"          # Cross-datacenter coordinators
    MANAGER = "manager"    # Datacenter-local job orchestrators
    WORKER = "worker"      # Load test generators (extreme load)
```

**Role Detection**:
- **Explicit**: Role gossiped in membership messages
- **Implicit**: Inferred from port range, hostname pattern, or configuration

### Role-Specific Confirmation Strategies

Each role has a tailored strategy for handling unconfirmed peers:

```python
@dataclass
class RoleBasedConfirmationStrategy:
    passive_timeout: float             # Base timeout before action
    enable_proactive_confirmation: bool # Whether to actively probe
    confirmation_attempts: int         # Number of retries
    attempt_interval: float           # Delay between retries
    latency_aware: bool               # Use Vivaldi for timeout adjustment
    use_vivaldi: bool                 # Enable Vivaldi coordinate system
    load_multiplier_max: float        # Max timeout multiplier under load
```

**Strategies by Role**:

| Role | Passive Timeout | Proactive Confirmation | Vivaldi | Load Multiplier | Rationale |
|------|----------------|------------------------|---------|-----------------|-----------|
| **Gate** | 120s | ✅ Yes (5 attempts) | ✅ Yes | 3x | Cross-DC, high-latency, need high confidence |
| **Manager** | 90s | ✅ Yes (3 attempts) | ✅ Yes | 5x | Moderate load, mission-critical |
| **Worker** | 180s | ❌ No | ❌ No | 10x | Extreme load, passive only (don't add more load) |

### Adaptive Timeout Calculation

For **Gates and Managers** (using Vivaldi):
```python
def get_adaptive_timeout(peer: NodeAddress, base_timeout: float) -> float:
    # Estimate RTT using Vivaldi coordinates
    estimated_rtt = vivaldi.estimate_rtt(peer)

    # Reference RTT (same-datacenter baseline)
    reference_rtt = 10.0  # ms

    # Latency multiplier
    latency_multiplier = min(10.0, max(1.0, estimated_rtt / reference_rtt))

    # Load multiplier (from LHM - existing system)
    load_multiplier = get_lhm_multiplier()

    # Confidence adjustment (higher error → more conservative)
    confidence_adjustment = 1.0 + (vivaldi.get_error() / 10.0)

    # Combined adaptive timeout
    return base_timeout * latency_multiplier * load_multiplier * confidence_adjustment
```

**Example**:
```python
# Base timeout: 5 seconds
# Gate in US-East detecting managers:

Manager in US-East:     estimated_rtt=5ms   → timeout = 5s × 1.0 × 1.0 × 1.05 = 5.25s
Manager in US-West:     estimated_rtt=50ms  → timeout = 5s × 5.0 × 1.0 × 1.08 = 27s
Manager in EU:          estimated_rtt=100ms → timeout = 5s × 10.0 × 1.2 × 1.12 = 67s
Manager in Asia:        estimated_rtt=200ms → timeout = 5s × 10.0 × 1.5 × 1.15 = 86s
                                                       (capped at max)
```

---

## Part 3: UNCONFIRMED Lifecycle State

### Current Problem (from AD-29)

Peers discovered via gossip are immediately marked `ALIVE`, but AD-29 prevents suspecting unconfirmed peers. This creates ambiguity:
- Is an unconfirmed peer "alive but not yet confirmed" or "dead but never joined"?
- How long do we wait before cleanup?

### Solution: Explicit UNCONFIRMED State

Add a new lifecycle state to the incarnation tracker:

```python
class NodeLifecycleState(Enum):
    UNCONFIRMED = b"UNCONFIRMED"  # Discovered but never confirmed
    ALIVE = b"ALIVE"               # Confirmed and healthy
    SUSPECT = b"SUSPECT"           # Suspected of failure
    DEAD = b"DEAD"                 # Confirmed dead
```

### State Transition Diagram

```
       [Gossip Discovery]
              ↓
         UNCONFIRMED ──────[role-aware timeout]──────→ [Removed from membership]
              ↓                                         (not marked DEAD)
      [First successful bidirectional
       communication: ping/ack]
              ↓
            ALIVE ──────[probe timeout]──────→ SUSPECT ──────[suspicion timeout]──────→ DEAD
              ↑                                    ↓
              └──────────[refutation]──────────────┘
```

**Key Transitions**:
1. **Discovery → UNCONFIRMED**: Peer added via gossip, no confirmation yet
2. **UNCONFIRMED → ALIVE**: First successful ping/ack (bidirectional confirmation)
3. **UNCONFIRMED → Removed**: Role-aware timeout expires without confirmation
4. **ALIVE → SUSPECT → DEAD**: Existing SWIM failure detection (unchanged)

---

## Part 4: Combined Architecture

### Component Diagram

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         HealthAwareServer                                │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │              VivaldiCoordinateSystem                            │    │
│  │  - Maintains own coordinate in virtual space                    │    │
│  │  - Updates coordinate on each ping/ack RTT measurement          │    │
│  │  - Estimates RTT to peers using coordinate distance             │    │
│  │  - Gossips coordinate in SWIM messages (50 byte overhead)       │    │
│  └────────────────────┬────────────────────────────────────────────┘    │
│                       │                                                  │
│                       ▼                                                  │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │         RoleAwareConfirmationManager                            │    │
│  │  - Classifies peers by role (Gate/Manager/Worker)               │    │
│  │  - Applies role-specific confirmation strategies                │    │
│  │  - Combines Vivaldi RTT + LHM load + confidence                 │    │
│  │  - Proactively confirms Gates/Managers, passive for Workers     │    │
│  └────────────────────┬────────────────────────────────────────────┘    │
│                       │                                                  │
│                       ▼                                                  │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │            IncarnationTracker (Enhanced)                        │    │
│  │  - Tracks node lifecycle: UNCONFIRMED → ALIVE → SUSPECT → DEAD  │    │
│  │  - New: UNCONFIRMED state for unconfirmed peers                 │    │
│  │  - Enforces AD-29: Only ALIVE peers can transition to SUSPECT   │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

### Workflow: Peer Discovery to Confirmation

```
1. Gate discovers Manager via gossip
   ├─> IncarnationTracker: Mark as UNCONFIRMED
   ├─> VivaldiCoordinateSystem: No coordinate yet (use conservative default)
   └─> RoleAwareConfirmationManager: Start passive timeout (120s for Gate role)

2. Gate sends SWIM ping to Manager
   ├─> Include Gate's Vivaldi coordinate in ping message
   └─> Measure RTT start time

3. Manager responds with ack
   ├─> Include Manager's Vivaldi coordinate in ack
   └─> Gate measures RTT: 145ms

4. Gate processes ack
   ├─> VivaldiCoordinateSystem.update_coordinate(manager, manager_coord, 145ms)
   │   ├─> Update Gate's position to minimize prediction error
   │   └─> Store Manager's coordinate for future distance calculations
   │
   ├─> IncarnationTracker: Transition Manager from UNCONFIRMED → ALIVE
   │   └─> Manager is now confirmed (successful bidirectional communication)
   │
   └─> RoleAwareConfirmationManager: Cancel passive timeout timer
       └─> Manager is confirmed, no cleanup needed

5. Future suspicion timeouts for this Manager
   ├─> VivaldiCoordinateSystem.estimate_rtt(manager) → 145ms (from coordinates)
   ├─> Calculate adaptive timeout: base × latency_multiplier × lhm × confidence
   └─> Use adaptive timeout for suspicion (e.g., 67s instead of 5s)
```

### Workflow: Unconfirmed Peer Cleanup

```
1. Gate discovers Manager via gossip (Manager never joins)
   ├─> IncarnationTracker: Mark as UNCONFIRMED
   └─> RoleAwareConfirmationManager: Start passive timeout (120s)

2. 60 seconds elapse, no confirmation
   └─> RoleAwareConfirmationManager: Check strategy for MANAGER role
       ├─> enable_proactive_confirmation = True
       ├─> confirmation_attempts = 3
       └─> Schedule proactive confirmation attempts

3. Attempt 1: Send ping for confirmation
   ├─> Wait 5 seconds for ack
   └─> No response

4. Attempt 2: Send ping for confirmation (5s later)
   ├─> Wait 5 seconds for ack
   └─> No response

5. Attempt 3: Send ping for confirmation (5s later)
   ├─> Wait 5 seconds for ack
   └─> No response

6. All attempts exhausted (135s total elapsed)
   ├─> RoleAwareConfirmationManager: Remove Manager from membership
   ├─> IncarnationTracker: Remove node (NOT marked as DEAD)
   ├─> Metrics: Increment "unconfirmed_peers_removed_manager"
   └─> Audit: Record UNCONFIRMED_PEER_REMOVED event
```

---

## Part 5: Benefits

### For Gates (Cross-Datacenter Detection)

**Before** (Static Timeouts):
```
Gate → Manager (US-East, 10ms):   30s timeout → Too conservative
Gate → Manager (US-West, 50ms):   30s timeout → Reasonable
Gate → Manager (EU, 150ms):       30s timeout → Too aggressive (false positives)
Gate → Manager (Asia, 300ms):     30s timeout → Very aggressive (many false positives)
```

**After** (Vivaldi + Role-Aware):
```
Gate → Manager (US-East, 10ms):   5s timeout   → Fast detection, no false positives
Gate → Manager (US-West, 50ms):   27s timeout  → Latency-adjusted
Gate → Manager (EU, 150ms):       67s timeout  → Accounts for cross-Atlantic latency
Gate → Manager (Asia, 300ms):     86s timeout  → Conservative for intercontinental
```

**Improvements**:
- ✅ **6x faster detection** for nearby peers
- ✅ **Zero false positives** from geographic latency
- ✅ **Automatic adaptation** to network topology changes

### For Managers (High Update Load)

**Before** (Static Timeouts + LHM):
```
Manager → Manager (under load):  30s × 2.5 LHM = 75s timeout
```

**After** (Vivaldi + LHM + Role-Aware):
```
Manager → Manager (same DC, under load):  5s × 1.0 latency × 2.5 LHM × 1.1 confidence = 13.75s

Benefits:
- Vivaldi detects same-DC peers (low latency) → Use tighter base timeout
- LHM scales for load spikes (existing mechanism preserved)
- Confidence adjustment prevents premature detection during convergence
```

**Improvements**:
- ✅ **5.4x faster detection** when both peers healthy
- ✅ **Graceful degradation** under load via LHM
- ✅ **No spurious failures** during Vivaldi convergence

### For Workers (Extreme Load)

**Before**:
```
Manager → Worker:  Proactive confirmation attempts add load to stressed worker
```

**After** (Passive-Only Strategy):
```
Manager → Worker:  180s passive timeout, no probing
                   Under extreme load: 180s × 10 LHM = 1800s (30 minutes)

Benefits:
- Workers never receive proactive confirmation probes
- Very high timeout tolerates multi-minute busy periods
- Workers are expendable (can be removed without suspicion/DEAD marking)
```

**Improvements**:
- ✅ **Zero additional load** on stressed workers
- ✅ **30-minute tolerance** for extreme load test scenarios
- ✅ **Clean removal** without protocol violations

---

## Part 6: Dual-Purpose Vivaldi (Failure Detection + Routing)

Vivaldi coordinates serve **two purposes** in the architecture:

### 1. Failure Detection (This AD)
- Adaptive timeouts for cross-datacenter suspicion
- Reduces false positives from geographic latency

### 2. Job Routing (Future: AD-36)
Gates can use Vivaldi to route jobs to optimal datacenters:

```python
class GateJobRouter:
    def select_datacenter_for_job(self, job_id: str) -> str:
        """
        Select datacenter using Vivaldi distance + health + load.
        """
        candidates = []

        for dc_name, dc_leader_addr in self.datacenter_leaders.items():
            # Filter unhealthy DCs
            if not self.is_datacenter_healthy(dc_name):
                continue

            # Estimate RTT to DC leader using Vivaldi
            estimated_rtt = self.vivaldi.estimate_rtt(dc_leader_addr)

            # Get DC load from gossip (LHM)
            dc_load = self.get_datacenter_load(dc_name)

            # Score = RTT × load (lower is better)
            # Balances "close and fast" with "not overloaded"
            score = estimated_rtt * dc_load

            candidates.append((dc_name, score))

        # Return DC with best score
        candidates.sort(key=lambda x: x[1])
        return candidates[0][0] if candidates else None
```

**Result**: Jobs routed to **closest available datacenter** based on learned network topology, not static configuration.

---

## Part 7: Implementation Phases

### Phase 1: Vivaldi Coordinate System (Standalone)
- ✅ Implement VivaldiCoordinateSystem class
- ✅ Integrate with SWIM ping/ack for RTT measurement
- ✅ Add coordinate to gossip messages (~50 byte overhead)
- ✅ Test coordinate convergence (10-20 rounds)

### Phase 2: UNCONFIRMED Lifecycle State
- ✅ Add UNCONFIRMED to NodeLifecycleState enum
- ✅ Update IncarnationTracker to support UNCONFIRMED → ALIVE transition
- ✅ Mark new peers as UNCONFIRMED on discovery
- ✅ Transition to ALIVE on first successful bidirectional communication

### Phase 3: Role-Aware Confirmation Strategies
- ✅ Implement PeerRole classification
- ✅ Define RoleBasedConfirmationStrategy per role
- ✅ Implement role-specific cleanup logic:
  - Gates: Proactive confirmation with 5 retries
  - Managers: Proactive confirmation with 3 retries
  - Workers: Passive removal only (no probes)

### Phase 4: Integration and Adaptive Timeouts
- ✅ Integrate Vivaldi RTT estimates with suspicion timeouts
- ✅ Combine Vivaldi latency multiplier + LHM load multiplier + confidence adjustment
- ✅ Update HierarchicalFailureDetector to accept adaptive timeouts
- ✅ Add metrics and observability

### Phase 5: Job Routing (Future - AD-36)
- ⏳ Implement GateJobRouter using Vivaldi distance
- ⏳ Add DC health + load balancing
- ⏳ Test cross-datacenter job routing

---

## Part 8: Tradeoffs and Limitations

### Tradeoffs

| Aspect | Benefit | Cost |
|--------|---------|------|
| **Vivaldi Overhead** | Adaptive timeouts, topology learning | 50-80 bytes per message |
| **Coordinate Convergence** | Accurate RTT prediction | 10-20 seconds initial convergence |
| **Role Classification** | Tailored strategies per role | Requires role detection logic |
| **UNCONFIRMED State** | Explicit lifecycle, clear semantics | Additional state to manage |
| **Proactive Confirmation** | Fewer false removals for Gates/Managers | Additional network probes |

### Limitations

1. **Vivaldi Accuracy**: Triangle inequality violations in real networks can reduce accuracy
   - **Mitigation**: Use height component to model asymmetric routes
   - **Impact**: ~10-20% RTT prediction error acceptable for timeout adjustment

2. **Role Detection**: Requires correct role classification
   - **Mitigation**: Multiple detection methods (explicit gossip, port range, config)
   - **Impact**: Misclassified role uses suboptimal strategy (still safe, just not optimal)

3. **Memory Overhead**: Storing coordinates for all peers
   - **Mitigation**: 4D coordinate = 40 bytes per peer (negligible)
   - **Impact**: For 1000 peers: 40KB total (insignificant)

4. **Cold Start**: New nodes have high error initially
   - **Mitigation**: Confidence adjustment makes timeouts more conservative during convergence
   - **Impact**: Slightly slower detection for first 10-20 seconds, then converges

---

## Part 9: Metrics and Observability

### New Metrics

```python
# Vivaldi metrics
vivaldi_coordinate_updates          # Counter: Coordinate update events
vivaldi_prediction_error            # Histogram: |predicted_rtt - measured_rtt|
vivaldi_convergence_time            # Histogram: Time to converge (error < threshold)

# Role-aware confirmation metrics
unconfirmed_peers_removed_gate      # Counter: Gates removed due to no confirmation
unconfirmed_peers_removed_manager   # Counter: Managers removed due to no confirmation
unconfirmed_peers_removed_worker    # Counter: Workers removed due to no confirmation
confirmation_attempts_total         # Counter: Proactive confirmation attempts
confirmation_attempts_success       # Counter: Successful late confirmations

# Lifecycle state metrics
peers_unconfirmed                   # Gauge: Peers currently in UNCONFIRMED state
peers_alive                         # Gauge: Peers currently in ALIVE state
peers_suspect                       # Gauge: Peers currently in SUSPECT state
peers_dead                          # Gauge: Peers currently in DEAD state
transitions_unconfirmed_to_alive    # Counter: UNCONFIRMED → ALIVE transitions
transitions_unconfirmed_to_removed  # Counter: UNCONFIRMED → Removed transitions

# Adaptive timeout metrics
adaptive_timeout_applied            # Histogram: Final adaptive timeout values
latency_multiplier                  # Histogram: Vivaldi latency multiplier
load_multiplier                     # Histogram: LHM load multiplier
confidence_adjustment               # Histogram: Vivaldi confidence adjustment
```

### Debug Endpoints

```python
# GET /debug/vivaldi/coordinate
{
    "position": [1.2, -0.5, 3.1, 0.8],
    "height": 0.3,
    "error": 0.15,
    "peer_count": 47,
    "convergence_status": "converged"
}

# GET /debug/vivaldi/peers
[
    {
        "peer": "10.0.1.5:8000",
        "estimated_rtt_ms": 145.3,
        "measured_rtt_samples": [143.1, 147.2, 145.5],
        "prediction_error_ms": 2.8,
        "adaptive_timeout_s": 67.2
    },
    ...
]

# GET /debug/peers/unconfirmed
[
    {
        "peer": "10.0.2.7:8000",
        "role": "manager",
        "discovered_at": "2026-01-10T10:23:45Z",
        "age_seconds": 47.3,
        "passive_timeout_remaining": 72.7,
        "confirmation_attempts": 1,
        "next_attempt_in": 5.0
    },
    ...
]
```

---

## Part 10: Success Criteria

This AD is successful when:

1. ✅ **Zero false positives from geographic latency**
   - Measured: `suspicions_started{reason="timeout"}` for cross-DC peers
   - Target: <1% false positive rate

2. ✅ **Faster detection for nearby peers**
   - Measured: Time from failure to detection for same-DC peers
   - Target: <10s (currently ~30s)

3. ✅ **No additional load on workers**
   - Measured: `confirmation_attempts_total{role="worker"}` = 0
   - Target: Zero proactive probes to workers

4. ✅ **Vivaldi convergence**
   - Measured: `vivaldi_prediction_error` < 20% of measured RTT
   - Target: Converges within 20 seconds of node start

5. ✅ **Clean unconfirmed peer removal**
   - Measured: `peers_unconfirmed` gauge remains bounded
   - Target: No unbounded growth over time

6. ✅ **Dual-purpose utility**
   - Measured: Vivaldi used for both failure detection AND job routing
   - Target: Single coordinate system serves both use cases

---

## Part 11: Related Work

### Vivaldi in Production Systems

1. **Serf/Consul (HashiCorp)**:
   - Uses Vivaldi for network tomography
   - Helps route RPC requests through nearby nodes
   - Documented: https://github.com/hashicorp/serf/blob/master/docs/internals/coordinates.html.markdown

2. **Cassandra**:
   - Uses Vivaldi-like coordinates for replica placement
   - Dynamic snitch adapts routing based on measured latency

3. **Research**:
   - Original Vivaldi paper: "Vivaldi: A Decentralized Network Coordinate System" (Dabek et al., SIGCOMM 2004)
   - 98% accuracy for predicting RTT in PlanetLab experiments

### Role-Aware Failure Detection

Inspired by:
- **Google Chubby**: Different timeout strategies for different client types
- **ZooKeeper**: Session timeout negotiation based on client capabilities
- **etcd**: Adaptive timeouts based on observed client latency

---

## Part 12: Alternatives Considered

### Alternative 1: Static Per-Datacenter Timeouts

**Approach**: Configure different timeouts for each datacenter pair manually.

**Pros**:
- ✅ Simpler implementation
- ✅ No coordinate system needed

**Cons**:
- ❌ Requires manual configuration for every datacenter pair (O(n²))
- ❌ Cannot adapt to network changes
- ❌ No learning of actual topology
- ❌ Doesn't help with job routing

**Verdict**: Rejected - doesn't scale, no adaptation.

### Alternative 2: Exponential Backoff for All Timeouts

**Approach**: Start with short timeout, double on each false positive.

**Pros**:
- ✅ Simple to implement
- ✅ Eventually converges to safe timeout

**Cons**:
- ❌ Many false positives during convergence
- ❌ Per-peer state required
- ❌ Doesn't distinguish legitimate slowness from failure
- ❌ No topology learning

**Verdict**: Rejected - too many false positives during learning phase.

### Alternative 3: Ping-Based Latency Measurement Only (No Vivaldi)

**Approach**: Measure RTT during pings, adjust timeouts based on measured RTT.

**Pros**:
- ✅ Simpler than Vivaldi
- ✅ Direct measurement is accurate

**Cons**:
- ❌ Cannot predict RTT to nodes you haven't measured yet
- ❌ No benefit for job routing (need to probe all candidates)
- ❌ Slower convergence (need N measurements for N peers)

**Verdict**: Rejected - Vivaldi provides prediction without measurement, crucial for routing.

### Alternative 4: Vivaldi Only (No Role-Aware Logic)

**Approach**: Use Vivaldi for all peers uniformly.

**Pros**:
- ✅ Simpler than role-aware logic
- ✅ Handles latency variance

**Cons**:
- ❌ Still probes stressed workers (adds load)
- ❌ Doesn't account for role-specific needs
- ❌ Workers don't benefit from Vivaldi (same-DC as manager)

**Verdict**: Rejected - role-aware logic is critical for worker protection.

---

## Conclusion

**AD-35 combines three orthogonal improvements** that together provide a robust, adaptive, globally-aware failure detection system:

1. **Vivaldi Coordinates**: Learn network topology, predict RTT, eliminate geographic false positives
2. **Role-Aware Strategies**: Tailor confirmation logic to peer role (Gate/Manager/Worker)
3. **UNCONFIRMED State**: Explicit lifecycle for unconfirmed peers, clean semantics

**Result**: A failure detection system that is:
- ✅ **Adaptive** to real network conditions
- ✅ **Role-aware** for optimal per-tier behavior
- ✅ **Dual-purpose** for both detection and routing
- ✅ **Production-proven** algorithms (Vivaldi used in Serf, Consul, Cassandra)
- ✅ **AD-29 compliant** (only confirmed peers can be suspected)

This architecture provides the foundation for globally-distributed, multi-tier failure detection at scale.