// Copyright 2026 The RocketMQ Rust Authors
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

#[test]
fn durable_authority_and_raw_queue_storage_are_not_publicly_forgeable() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/mapped_file_retirement_capability/*.rs");
}

#[test]
fn queue_removal_surface_is_confined_to_the_capability_boundary() {
    let queue = include_str!("../src/mapped_file/retirement/registry/queue_slot.rs").replace("\r\n", "\n");
    let production = queue
        .rsplit_once("#[cfg(test)]")
        .map(|(source, _)| source)
        .expect("queue-slot tests follow production code");

    assert!(production.contains("files: ArcSwap<Vec<Arc<T>>>"));
    assert!(production.contains("fn handoff_retirement"));
    assert!(production.contains("prepared.rollback()"));
    assert!(!production.contains("pub fn files"));
    assert!(!production.contains("pub fn remove"));
    assert!(!production.contains(".rcu("));
}
