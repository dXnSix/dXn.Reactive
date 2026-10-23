---
agentx:
  kind: state
  scope: knowledge/scalar-state
  status: current
  created: 2026-10-05T23:30:00+02:00
  updated: 2026-10-23T22:00:00+02:00
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

The crate root owns scalar/ViewModel state: `Signal<T>`, `ReadSignal<T>`, `SignalChange<T>`, `Computed<T>`, `Effect`, `EventBus<T>`/`EventStream<T>`, `Command`/`AsyncCommand`/`CommandState`, `ViewModelActivator`/`ActivationToken`/`ActivatableViewModel`, `DisposeBag`/`Disposable`/`DisposeWith`, and `ReactiveValueStreamExt`. The crate prelude re-exports them with the shared primitives. Scalar modules use `dxn_reactive::primitives` and never `dxn_reactive::collections` ([single crate decision](../../decisions/repository/2026-10-05-repository-single-reactive-crate.md)).

A signal commits state under its mutex and pushes its `SignalChange` before releasing it (only when the queue has subscribers), then drains after releasing it, so concurrent writers are delivered in commit order. Write paths are unwind-safe (`update` restores the previous value; `T::eq`/`T::clone` run before mutation) and the state lock recovers from poison. `values()` registers its change subscription before reading the baseline, delivers the baseline synchronously, buffers live changes (cloned) only until the baseline and buffered items are delivered on the subscribing thread, then forwards changes directly with a version check. `try_get`/`try_read` use `try_lock`.

`Computed` is a version-tuple core: it publishes a value only when the inputs' raw versions still equal the ones the evaluation read and the tuple supersedes the published one (push under the core lock, drain after). Input changes run a dirty/processing loop (one processing thread); reads go through a `SignalRefresh` hook on the signal (`Weak` to the core) that recomputes stale values on the reading thread. A thread-local evaluating set stops recursion; a re-entrant processor hands pending changes back to the refreshing frame. Construction evaluates once and re-processes only if input versions moved.

`Command`/`AsyncCommand` share an `Execution` (state, `is_executing` signal, in-flight count, latest generation, publish mutex used with deferred signal writes); an `ExecutionTicket` publishes the terminal state only for the latest generation and `Cancelled` from its drop guard. `ViewModelActivator` runs callbacks outside its state lock, uses generation IDs so a stale token cannot dispose a later activation, tracks an `Activating/Active/Ended` phase shared with tokens, abandons a generation on callback error or panic, and runs live/scoped registrations against a child bag that is disposed on failure.

GPUI bindings are not in this crate: RayX owns `ReactiveContextExt`/`GpuiBinding` (including the payload-free `notify_on`) in `../../RayX/RayX.Old/crates/rayx/src/reactive_gpui.rs`, re-exported as `rayx::reactive`. RayX's Test Suite page `apps/test_suite/src/pages/reactive_state.rs` with `tools-node/test_suite/rayx-reactive-state.spec.ts` is the headed WASM/WebGPU proof of the public scalar API (ordered values, computed updates, events, activation disposal).

## Backlinks

- [Signal](../../../../crates/src/signal.rs)
- [Computed](../../../../crates/src/computed.rs)
- [Activation](../../../../crates/src/activation.rs)
- [Commands](../../../../crates/src/command.rs)
- [Scalar unit tests](../../../../crates/src/tests.rs)
- [Scalar test groups](../../../../crates/src/scalar_tests/mod.rs)
- [Public API test](../../../../crates/tests/public_api.rs)
- 2026-10-05 current [One gpux-free dxn_reactive crate](../../decisions/repository/2026-10-05-repository-single-reactive-crate.md)
