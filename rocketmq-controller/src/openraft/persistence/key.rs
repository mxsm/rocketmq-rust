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

use std::borrow::Cow;

const LOG_PREFIX_V1: &str = "openraft/log/";

/// Domain identity for every persisted Controller Raft record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::openraft) enum RaftRecordKey {
    Vote,
    LastPurgedLog,
    Committed,
    LogEntry(u64),
    ReplicasInfoManagerState,
    LastApplied,
    LastMembership,
    SnapshotMeta,
    SnapshotData,
}

impl RaftRecordKey {
    pub(in crate::openraft) const fn class(self) -> &'static str {
        match self {
            Self::Vote => "vote",
            Self::LastPurgedLog => "last-purged-log",
            Self::Committed => "committed-log",
            Self::LogEntry(_) => "log-entry",
            Self::ReplicasInfoManagerState => "replica-state",
            Self::LastApplied => "last-applied",
            Self::LastMembership => "membership",
            Self::SnapshotMeta => "snapshot-meta",
            Self::SnapshotData => "snapshot-data",
        }
    }

    pub(in crate::openraft) fn as_v1_key(self) -> Cow<'static, str> {
        match self {
            Self::Vote => Cow::Borrowed("openraft/meta/vote"),
            Self::LastPurgedLog => Cow::Borrowed("openraft/meta/last_purged"),
            Self::Committed => Cow::Borrowed("openraft/meta/committed"),
            Self::LogEntry(index) => Cow::Owned(format!("{LOG_PREFIX_V1}{index:020}")),
            Self::ReplicasInfoManagerState => Cow::Borrowed("openraft/state_machine/replicas_info_manager"),
            Self::LastApplied => Cow::Borrowed("openraft/state_machine/last_applied"),
            Self::LastMembership => Cow::Borrowed("openraft/state_machine/last_membership"),
            Self::SnapshotMeta => Cow::Borrowed("openraft/state_machine/current_snapshot_meta"),
            Self::SnapshotData => Cow::Borrowed("openraft/state_machine/current_snapshot_data"),
        }
    }

    pub(in crate::openraft) const fn log_prefix_v1() -> &'static str {
        LOG_PREFIX_V1
    }

    pub(in crate::openraft) fn parse_v1_log_key(key: &str) -> Result<u64, std::io::Error> {
        let Some(suffix) = key.strip_prefix(LOG_PREFIX_V1) else {
            return Err(invalid_log_key());
        };
        if suffix.len() != 20 || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(invalid_log_key());
        }
        suffix.parse().map_err(|_| invalid_log_key())
    }
}

fn invalid_log_key() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "invalid persisted Controller log-entry key",
    )
}

#[cfg(test)]
mod tests {
    use super::RaftRecordKey;

    #[test]
    fn v1_keys_match_the_existing_persisted_namespace_exactly() {
        let cases = [
            (RaftRecordKey::Vote, "openraft/meta/vote"),
            (RaftRecordKey::LastPurgedLog, "openraft/meta/last_purged"),
            (RaftRecordKey::Committed, "openraft/meta/committed"),
            (RaftRecordKey::LogEntry(42), "openraft/log/00000000000000000042"),
            (
                RaftRecordKey::ReplicasInfoManagerState,
                "openraft/state_machine/replicas_info_manager",
            ),
            (RaftRecordKey::LastApplied, "openraft/state_machine/last_applied"),
            (RaftRecordKey::LastMembership, "openraft/state_machine/last_membership"),
            (
                RaftRecordKey::SnapshotMeta,
                "openraft/state_machine/current_snapshot_meta",
            ),
            (
                RaftRecordKey::SnapshotData,
                "openraft/state_machine/current_snapshot_data",
            ),
        ];

        for (key, expected) in cases {
            assert_eq!(key.as_v1_key(), expected);
        }
        assert_eq!(
            RaftRecordKey::parse_v1_log_key("openraft/log/00000000000000000042").expect("valid log key"),
            42
        );
    }

    #[test]
    fn malformed_v1_log_keys_fail_closed_without_echoing_the_key() {
        let error = RaftRecordKey::parse_v1_log_key("openraft/log/not-an-index").expect_err("malformed key must fail");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(!error.to_string().contains("not-an-index"));
    }
}
