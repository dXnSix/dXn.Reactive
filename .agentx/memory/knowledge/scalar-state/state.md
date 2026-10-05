---
agentx:
  kind: state
  scope: knowledge/scalar-state
  status: current
  created: 2026-10-05T23:30:00+02:00
  updated: 2026-10-05T23:30:00+02:00
  topics:
    - signals
    - computed
    - activation
    - commands
  supersedes: []
  superseded_by: []
---

# Scalar State

## Purpose And Usage

Read this before changing scalar reactive state, ViewModel activation, commands, effects, or event streams.

## Current Technical Shape

The crate root owns scalar/ViewModel state: `Signal<T>`, `ReadSignal<T>`, `SignalChange<T>`, `Computed<T>`, `Effect`, `EventBus<T>`/`EventStream<T>`, `Command`/`AsyncCommand`/`CommandState`, `ViewModelActivator`/`ActivationToken`/`ActivatableViewModel`, `DisposeBag`/`Disposable`/`DisposeWith`, and `ReactiveValueStreamExt`. The crate prelude re-exports them with the shared primitives. Scalar modules use `rayx_reactive::primitives` and never `rayx_reactive::collections` ([single crate decision](../../decisions/repository/2026-10-05-repository-single-reactive-crate.md)).

A signal commits state under its mutex and queues delivery after releasing it; `values()` registers its future-change subscription before publishing the synchronous current-value baseline, so changes committed during startup are not lost. `ViewModelActivator` runs callbacks outside its state lock and uses generation IDs so a stale token cannot dispose a later activation.

GPUI bindings are not in this crate: RayX owns `ReactiveContextExt`/`GpuiBinding` (including the payload-free `notify_on`) in `../RayX/crates/rayx/src/reactive_gpui.rs`, re-exported as `rayx::reactive`. RayX's Test Suite page `apps/test_suite/src/pages/reactive_state.rs` with `tools-node/test_suite/rayx-reactive-state.spec.ts` is the headed WASM/WebGPU proof of the public scalar API (ordered values, computed updates, events, activation disposal).

## Backlinks

- [Signal](../../../../crates/src/signal.rs)
- [Computed](../../../../crates/src/computed.rs)
- [Activation](../../../../crates/src/activation.rs)
- [Commands](../../../../crates/src/command.rs)
- [Scalar unit tests](../../../../crates/src/tests.rs)
- [Public API test](../../../../crates/tests/public_api.rs)
- 2026-10-05 current [One gpux-free rayx_reactive crate](../../decisions/repository/2026-10-05-repository-single-reactive-crate.md)
