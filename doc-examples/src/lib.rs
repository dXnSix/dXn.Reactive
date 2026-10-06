//! Test-only crate. `build.rs` turns every ```` ```rust ```` block in the repository's
//! `README.md` and `docs/*.md` into a test in `tests/doc_examples.rs`, so
//! `cargo test --workspace` keeps the documentation examples compiling and passing.
