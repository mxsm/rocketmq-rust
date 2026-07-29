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

//! Typed V1 persistence boundary for Controller OpenRaft state.

mod codec;
mod key;
mod log_repository;
mod state_repository;

pub(super) use codec::decode_v1;
pub(super) use codec::encode_v1;
pub(super) use key::RaftRecordKey;
pub(super) use log_repository::RaftLogRepository;
pub(super) use state_repository::RaftStateRepository;

use crate::error::ControllerError;

#[derive(Debug)]
struct PersistenceBackendError {
    operation: &'static str,
    key_class: &'static str,
    source: ControllerError,
}

impl std::fmt::Display for PersistenceBackendError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "controller persistence {} failed for {}",
            self.operation, self.key_class
        )
    }
}

impl std::error::Error for PersistenceBackendError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

fn backend_error(operation: &'static str, key: RaftRecordKey, error: ControllerError) -> std::io::Error {
    std::io::Error::other(PersistenceBackendError {
        operation,
        key_class: key.class(),
        source: error,
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn openraft_consumers_do_not_restore_raw_persistence_access() {
        let log_store = include_str!("../log_store.rs");
        let state_machine = include_str!("../state_machine.rs");
        let production_state_machine = state_machine
            .split("#[cfg(test)]")
            .next()
            .expect("production state machine");

        for source in [log_store, production_state_machine] {
            assert!(!source.contains("\"openraft/"));
            assert!(!source.contains(".write_batch("));
            assert!(!source.contains(".sync()"));
            assert!(!source.contains("serde_json::from_slice"));
        }
        assert_eq!(
            production_state_machine.matches("serde_json::to_vec").count(),
            1,
            "only the non-persistence inactive-broker response may use JSON directly"
        );
        assert!(production_state_machine.contains("serde_json::to_vec(&inactive_brokers)"));
    }
}
