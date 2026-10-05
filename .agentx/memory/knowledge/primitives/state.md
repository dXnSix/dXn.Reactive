---
agentx:
  kind: state
  scope: knowledge/primitives
  status: current
  created: 2026-10-05T23:30:00+02:00
  updated: 2026-10-05T23:30:00+02:00
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

## Backlinks

- [Primitives module](../../../../crates/src/primitives/mod.rs)
- [Delivery queue](../../../../crates/src/primitives/delivery.rs)
- [Value stream](../../../../crates/src/primitives/stream.rs)
- [Primitives public API test](../../../../crates/tests/primitives_public_api.rs)
- [Delivery benchmark](../../../../crates/benches/delivery_complexity.rs)
