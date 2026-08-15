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

use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;

use parking_lot::Mutex;

use crate::log_file::commit_log_path_set::StoreFaultInjector as StoreFaultInjectorContract;
pub use crate::log_file::commit_log_path_set::StoreFaultPoint;

/// Deterministic, Store-local failure schedule for integration tests.
#[derive(Debug, Default)]
pub struct StoreFaultInjector {
    state: Mutex<FaultState>,
}

#[derive(Debug, Default)]
struct FaultState {
    calls: BTreeMap<(StoreFaultPoint, PathBuf), usize>,
    fail_on: BTreeMap<(StoreFaultPoint, PathBuf), Vec<usize>>,
}

impl StoreFaultInjector {
    pub fn fail_on(&self, point: StoreFaultPoint, root: impl AsRef<Path>, invocation: usize) {
        assert!(invocation > 0, "fault invocation is one-based");
        let root = std::fs::canonicalize(root.as_ref()).expect("fault root must exist");
        self.state
            .lock()
            .fail_on
            .entry((point, root))
            .or_default()
            .push(invocation);
    }
}

impl StoreFaultInjectorContract for StoreFaultInjector {
    fn should_fail(&self, point: StoreFaultPoint, root: &Path) -> bool {
        let root = std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
        let key = (point, root);
        let mut state = self.state.lock();
        let invocation = {
            let calls = state.calls.entry(key.clone()).or_default();
            *calls += 1;
            *calls
        };
        state
            .fail_on
            .get(&key)
            .is_some_and(|scheduled| scheduled.contains(&invocation))
    }
}
