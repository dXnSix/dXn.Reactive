# RayX.Reactive

Reactive state for Rust UI applications, UI-framework neutral:

- **Scalar state** at the crate root: `Signal`, `ReadSignal`, `Computed`, `Effect`, `Command`,
  `AsyncCommand`, `EventBus`/`EventStream`, view-model activation and disposal.
- **`rayx_reactive::primitives`**: the shared `ValueStream`, `Subscription` and ordered
  `DeliveryQueue` primitives.
- **`rayx_reactive::collections`**: DynamicData-style observable collections (`SourceList`,
  `SourceCache`, typed `ChangeSet` streams) with filtering, sorting, grouping, joins, paging,
  virtualization windows and rxRust integration.

The `diagnostics` feature adds delivery/batching counters.

```toml
[dependencies]
rayx_reactive = "0.1"
```

GPUI bindings (`ReactiveContextExt`) live in RayX (`rayx::reactive`), not in this crate.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT)
at your option.
