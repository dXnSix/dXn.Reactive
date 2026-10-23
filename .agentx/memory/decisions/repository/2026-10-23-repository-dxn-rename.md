---
agentx:
  kind: decision
  scope: decisions/repository
  status: current
  created: 2026-10-23T22:00:00+02:00
  updated: 2026-10-23T22:00:00+02:00
  topics:
    - naming
    - release
    - consumers
  supersedes: []
  superseded_by: []
---

## Decision: The crate is `dxn_reactive` in `dXnSix/dXn.Reactive`, released from `v1.1.0`

### Context: On 2026-10-23 the owner moved RayX.Reactive, RayX.Dtcg and RayX.SignalR from the `rayx-ui` organization to the `dXnSix` account as dXn.Reactive, dXn.Dtcg and dXn.SignalR (GitHub redirects the old URLs) and asked for crate names starting with `dxn`. RayX stays in `rayx-ui` (its repository is now `RayX.Old`, checked out at `../../RayX/RayX.Old`) and remains a consumer.

### Alternatives considered: Keep `rayx_reactive` in the new repository; keep a re-exporting `rayx_reactive` shim; a hyphenated `dxn-reactive` package.

### Reasoning: The crate is named after its repository, as `dxn-signalr` and `dxn_dtcg` are, and keeps its underscore so only the prefix of every Rust path changes. The rename breaks consumers, so under the [1.x policy](2026-10-19-repository-release-plz-git-tags-one-x.md) (2026-10-19, current) it is a `feat` with a `Migration:` footer and ships as `v1.1.0`. release-plz's git-only mode looks the package up by its new name inside `v1.0.0` and cannot compute a next version, so `v1.1.0`'s version bump and changelog section were committed by hand; from `v1.1.0` on, release-plz finds the package again.

### Trade-offs accepted: Superseded decisions, `.agentx/tasks/` epics, the `1.0.0` changelog section and historical statements in current decisions keep the old names; read `rayx_reactive` and `rayx-ui/RayX.Reactive` there as `dxn_reactive` and `dXnSix/dXn.Reactive`. `RELEASE_PLZ_TOKEN` must now have `dXnSix` as its resource owner. RayX's own names (`rayx::reactive`, `rayx_diagnostics`, RayX paths) are unchanged.

### Supersedes: None.
