// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![recursion_limit = "256"]

use std::fs;
use std::path::Path;
use std::path::PathBuf;

const RECURSION_LIMIT_ATTRIBUTE: &str = "#![recursion_limit = \"";

fn top_level_rust_files(directory: &Path) -> Vec<PathBuf> {
    fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", directory.display()))
        .map(|entry| {
            entry
                .unwrap_or_else(|error| panic!("failed to read an entry in {}: {error}", directory.display()))
                .path()
        })
        .filter(|path| path.extension().is_some_and(|extension| extension == "rs"))
        .collect()
}

fn explicit_example_roots(manifest_dir: &Path) -> Vec<PathBuf> {
    let manifest_path = manifest_dir.join("Cargo.toml");
    let manifest = fs::read_to_string(&manifest_path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", manifest_path.display()));

    manifest
        .lines()
        .filter_map(|line| {
            line.trim()
                .strip_prefix("path = \"")
                .and_then(|path| path.strip_suffix('"'))
                .filter(|path| path.starts_with("examples/") && path.ends_with(".rs"))
                .map(|path| manifest_dir.join(path))
        })
        .collect()
}

#[test]
fn every_client_cargo_target_declares_a_recursion_limit() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut target_roots = vec![manifest_dir.join("src/lib.rs")];
    target_roots.extend(top_level_rust_files(&manifest_dir.join("tests")));
    target_roots.extend(top_level_rust_files(&manifest_dir.join("benches")));
    target_roots.extend(explicit_example_roots(&manifest_dir));
    target_roots.sort();
    target_roots.dedup();

    let missing = target_roots
        .iter()
        .filter(|path| {
            !fs::read_to_string(path)
                .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()))
                .contains(RECURSION_LIMIT_ATTRIBUTE)
        })
        .map(|path| path.strip_prefix(&manifest_dir).unwrap_or(path).display().to_string())
        .collect::<Vec<_>>();

    assert!(
        missing.is_empty(),
        "Cargo target crates must declare a recursion limit; missing:\n{}",
        missing.join("\n")
    );
}
