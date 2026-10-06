---
agentx:
  kind: state
  scope: knowledge/primitives
  status: current
  created: 2026-10-05T23:30:00+02:00
  updated: 2026-10-06T18:00:00+02:00
  topics:
    - value-stream
    - delivery-queue
    - subscription
  supersedes: []
  superseded_by: []
---

# Primitives State

## Purpose And Usage

Read this before changing shared streams, subscriptions, or ordered delivery used by both scalar state and collections.

## Current Technical Shape

`rayx_reactive::primitives` is the neutral layer under both scalar state and collections: generic `ValueStream<T, E>`, `ValueSubscriber<T>`, `Subscription`, `CompositeSubscription`, `DeliveryQueue<T>`, `ConnectionStatus`, `ReactiveError`, and `Result<T>`.

`DeliveryQueue<T>` is a transient, re-entrancy-safe ordered-delivery primitive. It accepts non-`Clone` payloads, gives snapshot subscribers borrowed references, and owns no replay or versioning semantics; collections layer their versioned startup handshake and keyed partitioning above it.

Publication is split into `push` (queue lock only, safe under the publisher's own state lock) and `drain` (one drainer at a time; others return and the running drain delivers their items); `enqueue = push + drain`. Publishers that commit under a lock push before unlocking and drain after, which makes delivery order equal commit order (signals and event buses do this). Subscribers live in subscription order with `u64` ids behind a lazily rebuilt `Arc<[Entry]>` snapshot; each entry's `active` flag is cleared by disposal before the lock and checked before every call, and removed entries/snapshots are dropped after unlocking (a subscriber closure may own other subscriptions to the same queue). A drain guard clears `delivering` on unwind, so a caught subscriber panic leaves the remaining items for the next drain. `pending_len()` exposes the backlog.

## Backlinks

- [Primitives module](../../../../crates/src/primitives/mod.rs)
- [Delivery queue](../../../../crates/src/primitives/delivery.rs)
- [Value stream](../../../../crates/src/primitives/stream.rs)
- [Primitives public API test](../../../../crates/tests/primitives_public_api.rs)
- [Delivery benchmark](../../../../crates/benches/delivery_complexity.rs)
- [Primitives delivery tests](../../../../crates/src/primitives/tests/mod.rs)
