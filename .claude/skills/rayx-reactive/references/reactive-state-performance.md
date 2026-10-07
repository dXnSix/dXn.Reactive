# RayX Reactive State: Performance, Testing, And Pitfalls

Read this when optimizing or reviewing `rayx_reactive` usage, testing ViewModels, or deciding whether state belongs in signals or observable collections.

Primary source files:

- `crates/src/*.rs`
- `crates/src/primitives/*.rs`
- `crates/src/collections/*.rs`
- `crates/src/tests.rs`

## Performance Rules

- Use `Signal<T>` for small current values; use `SourceList`/`SourceCache` for multi-item state.
- Use `set` to suppress duplicate scalar invalidations.
- Use `replace` only when equal assignments must still notify subscribers.
- Use `read` instead of `get` when a borrowed projection avoids cloning.
- Use `distinct_until_changed` before UI bindings for noisy streams.
- Use `Computed` to centralize repeated scalar derivations.
- Use collection aggregate streams for collection counts/sums/extrema instead of recomputing from snapshots in signal subscribers.
- Keep subscription callbacks short. Push expensive work into commands, async transforms, services, or collection operators.
- Keep `Subscription`, `Effect`, `ActivationToken`, or consumer binding (RayX `GpuiBinding`) values alive for the intended lifetime.
- Dispose subscriptions when views/routes/rows are evicted.
- UI consumers bind on their foreground task (RayX: `ReactiveContextExt`).
- For large bound values, stream IDs, `Arc<T>`, lightweight summaries, or collection changes instead of cloning full payloads per emission.

## Correctness Rules

- Use `values()` for initial render plus live updates.
- Use `changes()` only when previous/current/version is needed and late subscribers can miss current state.
- Remember `values()` can emit synchronously during subscription.
- Do not treat `EventBus` as state; events do not replay.
- For single-flight commands use `with_execution_policy(ExecutionPolicy::IgnoreWhileRunning)`; the default `Concurrent` policy starts overlapping executions. Bind busy state to `is_executing()`.
- Expect subscribers to run on whichever thread is draining the queue, not necessarily the writer's; marshal to the UI thread in the consumer.
- Do not call UI entity APIs from arbitrary stream callbacks.
- Do not make crate-root scalar modules depend on `rayx_reactive::collections`.

## Testing Patterns

Focused reactive tests should subscribe to streams, keep the returned subscription alive, mutate sources, and assert emitted values.

Use `Arc<Mutex<Vec<T>>>` only as a test probe or tiny utility backing store. Production collection views should generally use observable collection materialization or a consumer UI binding.

Test these boundaries:

- `values()` emits initial value.
- `changes()` does not emit initial value.
- `set` suppresses equal values.
- `replace` emits equal values.
- `Computed` updates when source signals change.
- `DisposeBag` disposes once.
- `ViewModelActivator` reference-counts activation and disposes on the last token drop.
- Commands set `Running`, `Succeeded`, `Failed`, or `Cancelled`; a disabled `execute` returns `Disabled` and publishes nothing.
- Timed operators (`debounce`, `throttle`, `sample`, `batch`, `expire_after`, ...) run on a `ManualScheduler` via their `*_with_scheduler` variants; advance it with `advance_by` instead of sleeping.

## Validation Commands

Run the touched test group while iterating, then the full list in the repository `AGENTS.md` once before committing:

```powershell
cargo test -p rayx_reactive --all-features --lib -- --skip collections::
cargo test -p rayx_reactive --all-features collections::tests::<group>
cargo clippy -p rayx_reactive --all-targets --all-features -- -D warnings
```

Never build or validate RayX (or another consumer) to test a change here; mimic the consumer scenario in this repository's tests and document breaking public-API changes in `CHANGELOG.md` with a *Migration:* note.

## DO

```rust
use rayx_reactive::prelude::*;
use std::sync::{Arc, Mutex};

#[test]
fn query_values_emit_initial_then_distinct_updates() -> Result<(), Box<dyn std::error::Error>> {
    let query = Signal::new("".to_string());
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen_for_subscriber = Arc::clone(&seen);

    let _subscription = query
        .values()
        .distinct_until_changed()
        .subscribe(move |value| {
            if let Ok(mut seen) = seen_for_subscriber.lock() {
                seen.push(value.clone());
            }
        })?;

    assert!(query.set("".to_string())?.is_none());
    assert!(query.set("rayx".to_string())?.is_some());

    Ok(())
}
```

## DON'T

```rust
use rayx_reactive::prelude::*;
use rayx_reactive::Subscription;

fn render_rows_bad(rows: &Signal<Vec<Row>>) -> rayx_reactive::Result<Subscription> {
    rows.values().subscribe(|rows| {
        // DON'T do full collection filtering, sorting, and rendering on every scalar emission.
        let mut visible = rows.clone();
        visible.retain(|row| row.visible);
        visible.sort_by(|a, b| a.title.cmp(&b.title));
        render_all_rows(&visible);
    })
}
```

Prefer `SourceCache` or `SourceList` with incremental collection operators and bind only the visible window.
