---
agentx:
  kind: decision
  scope: decisions/repository
  status: current
  created: 2026-10-05T23:30:00+02:00
  updated: 2026-10-05T23:30:00+02:00
  topics:
    - agentx-memory
    - taxonomy
    - transplant
    - sibling-repositories
  supersedes: []
  superseded_by: []
---

## Decision: Give RayX.Reactive its own AgentX memory with a module-ownership taxonomy, transplanted from RayX

### Context: `rayx_reactive` moved from RayX into this repository on 2026-10-05, but its memory (`runtime/reactive-state`, `runtime/collections`, nine decisions) and the `rayx-reactive-complexity-optimization` epic stayed in RayX's `.agentx/`. The collections knowledge file had grown to 38 KB mixing sources and operators, and some facts were stale (the GPUI binding and the `rayx_diagnostics` feature had already moved out of the crate).

### Alternatives considered: Keep the memory in RayX and point at it; copy RayX's two subdomains verbatim; or derive domains from this crate's module boundaries and redistribute the content.

### Reasoning: Memory belongs with the source it describes. The crate's module boundaries are its stable architecture, so domains follow them: `scalar-state` (crate root), `primitives`, `collections` (split into `sources` and `operators`, the two retrieval boundaries inside a large module), and `repository` (topology, packaging, validation, consumers, AgentX governance). Redistribution corrected stale claims against source: GPUI bindings and `notify_on` are RayX's (`rayx::reactive`), and diagnostics use the crate's `diagnostics` feature. The completed reactive epic moved with the memory. RayX keeps a slim `runtime/reactive-state` consumer view (re-exports, GPUI bindings, Test Suite proof, diagnostics forwarding) and drops `runtime/collections`.

### Trade-offs accepted: Decision filenames changed to the new prefixes; RayX epic text (for example `figma-dtcg-theme-parity`) keeps the old names, mapped below. Transplanted epic JSON was not edited, so its hashes and `crates/rayx/rayx_reactive*` paths refer to RayX. Excluded categories: no `ui`, `platform`, or `examples` domains, because the crate has no UI, platform code, or apps; The 16 crate-owned `reactive-*` guides from RayX's `rayx` skill moved into this repository's `rayx-reactive` skill (`.claude/skills/rayx-reactive/`) with crate-relative paths and `rayx_reactive` imports; RayX keeps a consumer guide and the GPUI bindings guide.

### Supersedes: None. RayX's decision index entries for the moved records now point here.

### Moved decisions

| RayX name (`decisions/runtime/`) | RayX.Reactive location |
|---|---|
| `collections/2026-06-24-runtime-collections-observable-collections.md` | [collections/2026-06-24-collections-observable-collections.md](../collections/2026-06-24-collections-observable-collections.md) |
| `collections/2026-06-27-runtime-collections-rayx-reactive-collections-crate.md` | [repository/2026-06-27-repository-rayx-reactive-collections-crate.md](2026-06-27-repository-rayx-reactive-collections-crate.md) |
| `reactive-state/2026-06-28-runtime-reactive-state-reactive-crate-split.md` | [repository/2026-06-28-repository-reactive-crate-split.md](2026-06-28-repository-reactive-crate-split.md) |
| `reactive-state/2026-10-05-runtime-reactive-state-single-reactive-crate.md` | [repository/2026-10-05-repository-single-reactive-crate.md](2026-10-05-repository-single-reactive-crate.md) |
| `collections/2026-07-24-runtime-collections-keyed-sort-indexes-and-batches.md` | [collections/operators/2026-07-24-collections-operators-keyed-sort-indexes-and-batches.md](../collections/operators/2026-07-24-collections-operators-keyed-sort-indexes-and-batches.md) |
| `collections/2026-07-24-runtime-collections-sparse-live-group-batches.md` | [collections/operators/2026-07-24-collections-operators-sparse-live-group-batches.md](../collections/operators/2026-07-24-collections-operators-sparse-live-group-batches.md) |
| `collections/2026-07-24-runtime-collections-keyed-source-cache-watch.md` | [collections/sources/2026-07-24-collections-sources-keyed-source-cache-watch.md](../collections/sources/2026-07-24-collections-sources-keyed-source-cache-watch.md) |
| `collections/2026-07-24-runtime-collections-nonblocking-wasm-cache-observers.md` | [collections/sources/2026-07-24-collections-sources-nonblocking-wasm-cache-observers.md](../collections/sources/2026-07-24-collections-sources-nonblocking-wasm-cache-observers.md) |
| `collections/2026-07-27-runtime-collections-history-free-targeted-delivery.md` | [collections/sources/2026-07-27-collections-sources-history-free-targeted-delivery.md](../collections/sources/2026-07-27-collections-sources-history-free-targeted-delivery.md) |
