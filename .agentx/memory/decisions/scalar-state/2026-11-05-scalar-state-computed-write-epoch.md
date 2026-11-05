---
agentx:
  kind: decision
  scope: decisions/scalar-state
  status: current
  created: 2026-11-05T12:00:00+01:00
  updated: 2026-11-05T12:00:00+01:00
  topics:
    - computed
    - freshness
    - performance
  supersedes: []
  superseded_by: []
---

## Decision: Computed freshness is gated by a write epoch of computed inputs

### Context: why this came up

Every `Computed` read and input check re-read all input versions fresh, refreshing upstream computeds recursively: O(depth^2) work per write through a chain (runtime-performance loop, experiments 25, 47, 49).

### Alternatives considered: what else was on the table

Keep the recursive check; per-core dirty propagation; a global epoch advanced by every observed write (tried first; it made writers on different threads contend on one cache line).

### Reasoning: why this option won

`signal.rs` keeps a process-wide `WRITE_EPOCH`, advanced under the signal lock after the version moves, only for signals marked `feeds_computed` by `computed::track`. A core records `verified` = the epoch read before its input check when it found itself fresh, or the epoch after its own publish when that publish advanced it by exactly one (the deferred writers report whether they advanced it). `refresh` returns immediately while the epoch equals `verified` and nothing is pending. Chains are about twice as fast and reads of an unchanged graph are O(1).

### Trade-offs accepted: what you gave up

Every write to a computed input pays one shared atomic increment. Any new path that can make a `Computed` stale must either write through a tracked signal or advance the epoch.

### Supersedes: link to prior decision, if replacing

None.
