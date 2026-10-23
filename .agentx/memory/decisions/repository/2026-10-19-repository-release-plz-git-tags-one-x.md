---
agentx:
  kind: decision
  scope: decisions/repository
  status: current
  created: 2026-10-19T01:30:00+02:00
  updated: 2026-10-23T22:00:00+02:00
  promoted_from: reactive-releases
  topics:
    - release
    - versioning
    - release-plz
    - changelog
  supersedes: []
  superseded_by: []
---

## Decision: release-plz cuts git-tag releases from Conventional Commits; the crate stays on 1.x

### Context: Epic `reactive-releases` (design decisions `d-release-plz-git-tags`, `d-one-x-policy`, `d-changelog-from-commits`, `d-consume-by-tag`), 2026-10-19, owner decisions of 2026-10-18. Builds on the [single crate decision](2026-10-05-repository-single-reactive-crate.md) (the crate stays `cargo package`-ready but is released through tags, not a registry) and on RayX.CLI's release-plz decision. First release: `v1.0.0` on `9f4a80e`.

### Decision text

`dxn_reactive` is released only as Git tags `vX.Y.Z`. release-plz (`release-plz.toml`, `.github/workflows/release-plz.yml`) runs git-only (`git_only`, `publish = false`, `git_release_enable = true`): on every push to `main` the `release` job tags a version that has no tag yet and creates the GitHub Release with its changelog section; `release-pr` (after `release`) keeps the release pull request. `doc-examples` has `release = false`; the root `CHANGELOG.md` is the package's changelog (`changelog_path`). Consumers depend on `{ git = "https://github.com/dXnSix/dXn.Reactive", tag = "vX.Y.Z" }`, never a branch, `rev` or path.

**1.x policy** (while the owner is the only consumer): `release_commits = "^(feat|fix|perf)"`, `semver_check = false`; a breaking change is a `feat` (or `fix`) whose body states the break and whose `Migration:` footer holds the consumer note, rendered as *Migration:* under its changelog entry. Never `!` or `BREAKING CHANGE:` (release-plz would propose 2.0.0 and cannot cap it). A release is merging the release PR after checking it is still 1.x, changes only `Cargo.toml`/`Cargo.lock`/`CHANGELOG.md` and its CI is green. The policy ends when a second consumer appears.

Commits are the changelog: `CHANGELOG.md` starts byte for byte with the configured header and is never hand-edited between releases. `RELEASE_PLZ_TOKEN` is a fine-grained PAT whose **resource owner is the repositories' owner, `dXnSix`** (it was `rayx-ui` until the repositories moved on 2026-10-23; a token for another owner cannot reach them, and the first run failed with a 403 at tag creation until the token was replaced). Changes are pushed straight to `main` (`fetch`, `rebase`, `--force-with-lease`); pull requests exist only for releases. Details: `docs/releasing.md`.

### Invariants

- Every proposed or released version is 1.x; no commit after `v1.0.0` carries `!` or a `BREAKING CHANGE:` footer.
- `v*` tags come only from release-plz or the documented by-hand fallback and are never moved or deleted ("Release tags" ruleset).
- `release-plz.yml` runs no tests; `ci.yml` runs on pull requests and pushes to `main`.
- Nothing is published to crates.io; `cargo package --workspace` keeps passing.

### Supersedes: None.
