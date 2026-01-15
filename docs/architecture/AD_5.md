---
ad_number: 5
name: Pre-Voting for Split-Brain Prevention
description: Leader election uses a pre-vote phase before the actual election
---

# AD-5: Pre-Voting for Split-Brain Prevention

**Decision**: Leader election uses a pre-vote phase before the actual election.

**Rationale**:
- Pre-vote doesn't increment term (prevents term explosion)
- Candidate checks if it would win before disrupting cluster
- Nodes only grant pre-vote if no healthy leader exists

**Implementation**:
- `_run_pre_vote()` gathers pre-votes without changing state
- Only proceeds to real election if pre-vote majority achieved
- If pre-vote fails, election is aborted
