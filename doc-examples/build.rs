//! Generates one test per ```` ```rust ```` block in `README.md` and `docs/*.md`.
//!
//! Each block becomes the body of a test returning `Result<(), Box<dyn Error>>`, so
//! examples can use `?`. A block directly preceded by `<!-- skip-check -->` is a
//! deliberate fragment and is skipped.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::{env, fs};

const SKIP_MARKER: &str = "<!-- skip-check -->";

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap_or_default());
    let repository = manifest_dir.parent().unwrap_or(&manifest_dir).to_path_buf();
    let docs = repository.join("docs");

    println!("cargo:rerun-if-changed=build.rs");
    println!(
        "cargo:rerun-if-changed={}",
        repository.join("README.md").display()
    );
    println!("cargo:rerun-if-changed={}", docs.display());

    let mut sources = vec![("readme".to_string(), repository.join("README.md"))];
    let mut chapters = fs::read_dir(&docs)
        .map(|entries| {
            entries
                .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                .filter(|path| path.extension().is_some_and(|extension| extension == "md"))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    chapters.sort();
    for chapter in chapters {
        println!("cargo:rerun-if-changed={}", chapter.display());
        sources.push((module_name(&chapter), chapter));
    }

    let mut generated = String::new();
    for (module, path) in sources {
        // Missing files (for example in a packaged crate) contribute no tests.
        let Ok(markdown) = fs::read_to_string(&path) else {
            continue;
        };
        let blocks = rust_blocks(&markdown);
        if blocks.is_empty() {
            continue;
        }
        let _ = writeln!(generated, "mod {module} {{");
        for (line, code) in blocks {
            let _ = writeln!(generated, "#[test]");
            let _ = writeln!(
                generated,
                "fn line_{line}() -> ::std::result::Result<(), ::std::boxed::Box<dyn ::std::error::Error>> {{"
            );
            let _ = writeln!(generated, "{code}");
            let _ = writeln!(generated, "#[allow(unreachable_code)]");
            let _ = writeln!(generated, "::std::result::Result::Ok(())");
            let _ = writeln!(generated, "}}");
        }
        let _ = writeln!(generated, "}}");
    }

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").unwrap_or_default());
    if let Err(error) = fs::write(out_dir.join("doc_examples.rs"), generated) {
        panic!("failed to write generated doc examples: {error}");
    }
}

/// `docs/03-signals-and-computed.md` becomes `docs_03_signals_and_computed`.
fn module_name(path: &Path) -> String {
    let stem = path
        .file_stem()
        .map(|stem| stem.to_string_lossy().to_lowercase())
        .unwrap_or_default();
    let mut name = String::from("docs_");
    for character in stem.chars() {
        name.push(if character.is_ascii_alphanumeric() {
            character
        } else {
            '_'
        });
    }
    name
}

/// Returns `(first code line number, code)` for every checked ```` ```rust ```` block.
fn rust_blocks(markdown: &str) -> Vec<(usize, String)> {
    let lines = markdown.lines().collect::<Vec<_>>();
    let mut blocks = Vec::new();
    let mut previous_non_blank = "";
    let mut index = 0;
    while index < lines.len() {
        let line = lines[index].trim();
        if line == "```rust" {
            let start = index + 1;
            let mut end = start;
            while end < lines.len() && lines[end].trim() != "```" {
                end += 1;
            }
            if previous_non_blank != SKIP_MARKER {
                blocks.push((start + 1, lines[start..end].join("\n")));
            }
            previous_non_blank = "```";
            index = end + 1;
            continue;
        }
        if !line.is_empty() {
            previous_non_blank = line;
        }
        index += 1;
    }
    blocks
}
