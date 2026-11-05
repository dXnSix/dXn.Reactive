---
agentx:
  kind: decision
  scope: decisions/collections
  status: current
  created: 2026-11-05T12:00:00+01:00
  updated: 2026-11-05T12:00:00+01:00
  topics:
    - hashing
    - performance
  supersedes: []
  superseded_by: []
---

## Decision: Collection tables hash with a seeded folded-multiply hasher

### Context: why this came up

SipHash (`RandomState`) was the top self-time cost in every collection profile: each operator mirrors its input in hash tables and hashes every key several times per change (runtime-performance improvement loop, experiment 16).

### Alternatives considered: what else was on the table

Keep SipHash; depend on `ahash`/`foldhash`; an unseeded Fx-style hasher.

### Reasoning: why this option won

`crates/src/collections/hash.rs` defines crate-internal `HashMap`/`HashSet` aliases over `FastState`: one 64x64->128-bit folded multiply per word, state seeded once per process from `RandomState`, so colliding keys cannot be chosen without the seed. No new dependency, no public type exposes the tables. Loads through caches, joins, sorts, groups and trees got 28-65% faster.

### Trade-offs accepted: what you gave up

Weaker than SipHash against an adversary who can observe timings over many keys; acceptable for in-process UI state. New collection code must import `HashMap`/`HashSet` from `crate::collections::hash` and build them with `default()`/`with_capacity_and_hasher`, never `new()`.

### Supersedes: link to prior decision, if replacing

None.
