---
agentx:
  kind: rules
  scope: knowledge/collections/sources
  status: current
  created: 2026-06-24T14:10:00+02:00
  updated: 2026-10-07T12:00:00+02:00
  topics:
    - source-list
    - source-cache
    - history-free
  supersedes: []
  superseded_by: []
---

# Collection Sources Rules

- Implement `SourceCache::watch(key)` with one shared keyed-delivery bridge: snapshot only the requested key, partition each live batch once by touched key, and invoke only that key's registered subscribers.
- Keep source and live-group replay history at exactly zero. Close snapshot/live races with a connection-local versioned startup buffer that subscribes first, snapshots current state/version, discards covered envelopes, drains newer envelopes, and then becomes live; keep discarding envelopes at or below the snapshot version after going live, because a batch committed before the snapshot can still arrive late (queued behind an in-progress delivery or enqueued by another thread after its commit); never reintroduce source-owned batch history.
- Treat `retained_implicit_batch_count()` as an application-wide concurrent diagnostic aggregate. Test connection lifecycle against connection-local buffered state and test atomic accounting with an isolated counter; do not assert exact global values while unrelated collection tests can run in parallel.
- Dispose keyed watches by removing their registration and pruning empty key buckets, dropping the removed subscribers after unlocking. `NestedUpdate` means same-thread re-entry only; cross-thread setup contention blocks on native and returns `CollectionError::Contended` (wasm32) without leaving partial registrations.
- Route every source and live-group state access through `SourceLock`; never call user code (subscribers, key selectors beyond the edit, `T::clone` of published data) while holding a raw `lock_blocking`/untracked guard, and push publications under the lock but drain them after unlocking.
- Keep list diffs localized and bounded: use indexed replacements for equal-length sparse changes, reserve `Clear` for an empty target, and use a bounded middle diff with structured remove/add fallback for broad non-empty rewrites.
- Build snapshot-to-changeset bridges on the same diff helpers used by live operators so snapshot streams and derived live views preserve identical minimal-change behavior.
- Source `load` APIs are snapshot replacement primitives: clear existing state and add the supplied snapshot inside one edit. Use `edit_diff` instead when the goal is minimal structural churn.
- Custom equality edit-diff paths (`edit_diff_by`, `edit_diff_list_by`, `edit_diff_optional_by`) must retain the previous value when the equality function suppresses an update; source/operator state should match what downstream has actually been told, not the unseen semantic-equal snapshot payload.
- Cache bind/materialization code must apply indexed `Update` changes as remove-at-previous-index plus insert-at-current-index when those indexes differ; DynamicData sorted cache streams use `Update` rather than remove/add for item updates that reposition in sorted order.
- Operators that materialize or populate keyed cache streams must apply the explicit incoming `CacheChange` key instead of recomputing keys from item values.
- Equality-aware cache upserts must skip existing-key updates when the comparer says previous and current values are equal; do not emit empty update batches that would congest derived views.
- Value-stream-to-changeset bridge operators must subscribe the downstream/private-source output before subscribing to upstream input, because `ValueStream` sources can emit synchronously during subscription.
- Source-driver factory streams (`create_list_change_stream`, `create_cache_change_stream`) must subscribe the private source `connect()` output before running the caller driver, because drivers may synchronously seed the source.
- Prefer one typed options object for Rust value-stream-to-changeset adapter controls instead of adding every DynamicData overload shape; keep limit and expiration behavior attached to the adapter's private backing source.
