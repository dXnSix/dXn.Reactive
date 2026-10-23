# Releasing `dxn_reactive`

A release is a version tag `vX.Y.Z` on `main` with a GitHub Release of the same name. Nothing is
published to crates.io: consumers depend on the tag,

```toml
[dependencies]
dxn_reactive = { git = "https://github.com/dXnSix/dXn.Reactive", tag = "vX.Y.Z" }
```

and `Cargo.lock` records the commit it points to. The manifests stay crates.io-ready
(`cargo package --workspace` passes in CI), but no registry release is made.

**release-plz** does all of the work (`release-plz.toml`, `.github/workflows/release-plz.yml`). It
runs in git-only mode (`git_only = true`, `publish = false`): versions come from the existing tags,
and on every push to `main` it

- keeps a release pull request with the next version and its `CHANGELOG.md` section, and
- when `Cargo.toml` holds a version that has no tag yet, tags `vX.Y.Z` and creates the GitHub
  Release with that version's `CHANGELOG.md` section as its notes.

Only `dxn_reactive` is released. The `doc-examples` test crate is never versioned
(`release = false`).

## How a release happens

1. Commit to `main` with [Conventional Commits](https://www.conventionalcommits.org/). The type
   decides the next version (see [the 1.x policy](#the-1x-policy)):

   | Commit | Release |
   | --- | --- |
   | `fix:`, `perf:` | patch (`1.0.0` → `1.0.1`) |
   | `feat:` | minor (`1.0.0` → `1.1.0`), also for a breaking change |
   | `docs:`, `test:`, `ci:`, `build:`, `refactor:`, `chore:` | none on their own (`release_commits` lists `feat`, `fix` and `perf`); they appear in the next release's changelog |

   `chore(agentx)`, `chore(release)` and `chore(deps)` commits are left out of the changelog.
2. On every push to `main`, release-plz opens or updates the release pull request: the version
   bump in `Cargo.toml` and `Cargo.lock` and a new `## [X.Y.Z] - YYYY-MM-DD` section at the top of
   `CHANGELOG.md`, generated from the commits. If a line reads badly, edit the changelog text in
   the pull request; between releases nobody edits `CHANGELOG.md` by hand.
3. `ci.yml` runs on the release pull request like on any pull request.
4. Check it, then merge it:
   - the proposed version is still `1.x`;
   - it changes only `Cargo.toml`, `Cargo.lock` and `CHANGELOG.md`;
   - its CI is green.
5. On the merge's push the `release` job finds a version with no tag, tags `vX.Y.Z` and creates
   the GitHub Release with the new section as notes. The `release-pr` job runs after it and reports
   that nothing is pending until the next releasable commit.

The first release, `v1.0.0`, needed no pull request: `Cargo.toml` already said `1.0.0` and no
`v*` tag existed, so release-plz treated it as the initial release and the `release` job tagged it
on the first push that contained the workflow. Its changelog section holds the hand-written notes
for the changes since the untagged 0.1 sources.

`v1.1.0`, which renamed the crate from `rayx_reactive` to `dxn_reactive`, was cut with the
[by-hand fallback](#by-hand-fallback): in git-only mode release-plz looks the package up by its
new name inside `v1.0.0`, finds nothing and cannot compute a next version. From `v1.1.0` on it
finds the package again.

## The 1.x policy

The crate stays on `1.x.x` while the owner is its only consumer. release-plz cannot cap a major
bump: it proposes `2.0.0` for a `!` commit, for a `BREAKING CHANGE:` footer, and for an API break
that cargo-semver-checks reports. So, until the policy ends:

- `fix` and `perf` release a patch, `feat` a minor; other types release nothing on their own.
- **A breaking change is a `feat`** (or a `fix`, when it is a fix) whose body states the break and
  whose `Migration:` footer holds the note for consumers. The changelog renders the footer as a
  *Migration:* line under the entry:

  ```text
  feat(collections): replace the paging options with a page request

  `page()` now takes a `PageRequest` signal; `PageOptions` is gone.

  Migration: build a `PageRequest::new(page, size)` signal and pass it to `page()`.
  ```

- **Never** write `feat!`/`fix!` or a `BREAKING CHANGE:` footer, and `semver_check = false` in
  `release-plz.toml`.
- Before merging a release pull request, check that its version is still `1.x`. A `2.0.0`
  proposal means a stray `!` or `BREAKING CHANGE:` slipped in: reword that commit on `main` (force
  push allowed, see [Pushing](#pushing)) instead of merging, and release-plz updates the pull
  request on the next push.

The policy ends when a second consumer appears. Then `!`, major bumps and the semver check return,
and this document, `release-plz.toml` and the contributor guidance change with it.

## The `RELEASE_PLZ_TOKEN` secret

Events made with the workflow's default `GITHUB_TOKEN` start no other workflows, so with it the
release pull request would get no CI. release-plz therefore uses a fine-grained personal access
token stored as the repository secret `RELEASE_PLZ_TOKEN`; it exists only in GitHub Actions.

It is one token shared by the `dXnSix` library repositories:

- resource owner `dXnSix` (the account that owns the repositories); repository access: the library repositories (this one included);
- repository permissions: **Contents** read and write, **Pull requests** read and write (Metadata
  read is added automatically);
- an expiry of at most one year.

Release pull requests, tags, releases and the commits release-plz makes appear as the token's
owner.

**Failure modes.** A repository missing from the token's access list fails at checkout or push of
the first Release-plz run. **When the token expires, nothing fails loudly:** both jobs fail at
checkout (the Actions tab shows red Release-plz runs), no release pull request is updated and no
tag is made; releases silently stop until the token is replaced.

**Rotation.** Before the expiry date, generate a new token with the same settings (or extend the
old one) and replace the secret's value in every repository that uses it (Settings → Secrets and
variables → Actions → `RELEASE_PLZ_TOKEN`). Nothing else changes.

## Release tags are immutable

The "Release tags" ruleset protects `refs/tags/v*`: tags can be created, but not moved
(`update`, `non_fast_forward`) or deleted, and nobody bypasses it. A bad release is fixed forward
with a new patch release, never by retagging.

## By-hand fallback

If release-plz cannot cut a release (a broken workflow, an expired token you cannot replace yet),
release by hand on an up-to-date `main`, with the owner's go-ahead:

```bash
# 1. Set [workspace.package] version in Cargo.toml and refresh the lockfile
cargo update -p dxn_reactive --offline
# 2. Add a `## [X.Y.Z] - YYYY-MM-DD` section under the header of CHANGELOG.md
# 3. Commit and push it (see Pushing), then tag that commit and push the tag
git tag vX.Y.Z
git push origin vX.Y.Z
# 4. Create the GitHub Release with the section as its notes (release-plz normally does this)
awk '/^## \[X\.Y\.Z\]/ { found = 1; next } /^## \[/ && found { exit } found' CHANGELOG.md > notes.md
gh release create vX.Y.Z --repo dXnSix/dXn.Reactive --title vX.Y.Z --notes-file notes.md
```

Tags always have the form `vX.Y.Z`. release-plz continues from the by-hand tag on its next run.

## Pushing

Changes go straight to `main`; pull requests exist only as release pull requests. Rebase on the
remote first and push with a lease, so a release-plz merge that landed in between is never
overwritten:

```bash
git fetch origin && git rebase origin/main
```

```bash
git push --force-with-lease origin main
```

`ci.yml` runs on every push to `main` and on every pull request; `release-plz.yml` runs next to it
on pushes and runs no tests.

## Checking a release

```bash
gh release view vX.Y.Z --repo dXnSix/dXn.Reactive
```

```bash
git ls-remote --tags https://github.com/dXnSix/dXn.Reactive 'v*'
```

Consumers pick up a release by changing `tag = "vX.Y.Z"` in their `Cargo.toml`; `cargo update -p
dxn_reactive` then records the new commit in `Cargo.lock`. Read the release's changelog section
first: every breaking change has a *Migration:* note.

---

← [Contributing](20-contributing.md) · [Back to the table of contents](README.md)
