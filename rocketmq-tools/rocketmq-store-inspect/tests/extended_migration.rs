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

use std::fs;
use std::process::Command;

/// A Broker configuration whose `[store]` section names `store_root`.
fn broker_config(dir: &std::path::Path, store_root: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("broker.toml");
    fs::write(
        &path,
        format!(
            "[store]\nstorePathRootDir = {:?}\ntimerStoreMode = \"native\"\n",
            store_root.to_string_lossy()
        ),
    )
    .expect("write the Broker configuration");
    path
}

#[test]
fn a_store_the_extended_timeline_does_not_own_is_left_alone() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let store_root = dir.path().join("store");
    fs::create_dir_all(&store_root).expect("create the store root");
    let config = broker_config(dir.path(), &store_root);

    let output = Command::new(env!("CARGO_BIN_EXE_rocketmq-cli-rust"))
        .args(["migrate-extended-timers", "--dry-run", "--config"])
        .arg(&config)
        .output()
        .expect("run rocketmq-cli-rust migrate-extended-timers");

    assert!(!output.status.success());
    assert!(output.stdout.is_empty(), "no report without a migration");
    assert_eq!(
        fs::read_dir(&store_root).expect("read the store root").count(),
        1,
        "only the lock file is created"
    );
}
