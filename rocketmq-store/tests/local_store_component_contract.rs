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

const FACADE: &str = include_str!("../src/message_store/local_file_message_store.rs");
const COMPOSITION: &str = include_str!("../src/message_store/local_file_message_store/composition.rs");
const READ_PATH: &str = include_str!("../src/message_store/local_file_message_store/read_path.rs");
const WRITE_PATH: &str = include_str!("../src/message_store/local_file_message_store/write_path.rs");
const DISPATCH: &str = include_str!("../src/message_store/local_file_message_store/dispatch.rs");
const RECOVERY: &str = include_str!("../src/message_store/local_file_message_store/recovery.rs");
const HEALTH: &str = include_str!("../src/message_store/local_file_message_store/health.rs");
const LIFECYCLE: &str = include_str!("../src/message_store/local_file_message_store/lifecycle.rs");

#[test]
fn canonical_local_store_has_explicit_capability_modules() {
    for module in [
        "composition",
        "read_path",
        "write_path",
        "dispatch",
        "recovery",
        "health",
        "lifecycle",
    ] {
        assert!(
            FACADE.contains(&format!("mod {module};")),
            "LocalFileMessageStore must declare the {module} module"
        );
    }

    assert!(FACADE.lines().count() <= 2_500);
    assert!(!FACADE.contains("mod tests {"));
    assert!(FACADE.contains("#[path = \"../../tests/message_store/local_file_message_store/unit.rs\"]"));
}

#[test]
fn core_facade_operations_delegate_to_capability_modules() {
    for delegation in [
        "self.load_store().await",
        "self.start_store().await",
        "self.initialize_store().await",
        "self.shutdown_store_gracefully().await",
        "self.read_messages(",
        "self.read_messages_with_size_limit(",
        "self.query_messages(",
        "self.append_replica_bytes(",
        "self.store_health_snapshot()",
    ] {
        assert!(
            FACADE.contains(delegation),
            "LocalFileMessageStore facade must delegate through {delegation}"
        );
    }

    assert!(COMPOSITION.contains("pub fn try_new("));
    assert!(READ_PATH.contains("async fn read_messages_with_size_limit("));
    assert!(WRITE_PATH.contains("async fn put_message_shared("));
    assert!(WRITE_PATH.contains("async fn append_replica_bytes("));
    assert!(DISPATCH.contains("struct ReputMessageService"));
    assert!(RECOVERY.contains("async fn recover("));
    assert!(HEALTH.contains("fn store_health_snapshot("));
    assert!(LIFECYCLE.contains("async fn shutdown_store_gracefully("));
}

#[test]
fn capability_modules_do_not_retain_an_additional_store_root() {
    for (name, source, line_limit) in [
        ("composition", COMPOSITION, 800),
        ("read_path", READ_PATH, 800),
        ("write_path", WRITE_PATH, 800),
        ("dispatch", DISPATCH, 1_200),
        ("recovery", RECOVERY, 800),
        ("health", HEALTH, 800),
        ("lifecycle", LIFECYCLE, 800),
    ] {
        assert!(
            source.lines().count() <= line_limit,
            "{name} exceeds its review threshold"
        );
        assert!(
            !source.contains("struct LocalFileMessageStore"),
            "{name} must not define a second LocalFileMessageStore root"
        );
        assert!(
            !source.contains("Arc<LocalFileMessageStore"),
            "{name} must not retain the complete LocalFileMessageStore"
        );
        assert!(
            !source.contains("Box<LocalFileMessageStore"),
            "{name} must not retain the complete LocalFileMessageStore"
        );
    }
}

#[test]
fn background_work_stays_with_the_injected_store_runtime_scope() {
    let owned_background_work = [DISPATCH, RECOVERY, HEALTH, LIFECYCLE].join("\n");

    assert!(owned_background_work.contains("StoreRuntimeScope"));
    assert!(owned_background_work.contains("task_group"));
    assert!(!owned_background_work.contains("tokio::spawn("));
    assert!(!owned_background_work.contains("tokio::task::spawn_blocking("));
    assert!(!owned_background_work.contains("std::thread::spawn("));
}
