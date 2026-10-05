---
agentx:
  kind: knowledge
  scope: knowledge/scalar-state
  status: current
  created: 2026-10-05T23:30:00+02:00
  updated: 2026-10-05T23:30:00+02:00
  topics:
    - signals
    - computed
  supersedes: []
  superseded_by: []
---

# Scalar State Knowledge

- `Signal<T>::values()` emits the current value immediately and then future current values; `Signal<T>::changes()` emits only future `SignalChange<T>` values with previous/current/version payloads.
- `Signal<T>::set` suppresses equal values when `T: PartialEq`; `replace` always advances the signal version and emits a change.
- `Computed<T>` owns its subscriptions in a `DisposeBag`, so a derived signal stops updating when the computed value is dropped; derivation closures run outside internal coordinator mutexes.
- `Signal::values()` returns a primitives `ValueStream<_, ReactiveError>`, which collection operators accept directly (see [primitives knowledge](../primitives/knowledge.md)).
