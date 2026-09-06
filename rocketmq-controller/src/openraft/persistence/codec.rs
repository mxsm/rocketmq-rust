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

use serde::de::DeserializeOwned;
use serde::Serialize;

use super::RaftRecordKey;

#[derive(Debug)]
struct PersistenceCodecError {
    operation: &'static str,
    key_class: &'static str,
    source: serde_json::Error,
}

impl std::fmt::Display for PersistenceCodecError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "controller persistence {} failed for {}",
            self.operation, self.key_class
        )
    }
}

impl std::error::Error for PersistenceCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

pub(in crate::openraft) fn encode_v1<T: Serialize>(key: RaftRecordKey, value: &T) -> Result<Vec<u8>, std::io::Error> {
    serde_json::to_vec(value).map_err(|source| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            PersistenceCodecError {
                operation: "encode",
                key_class: key.class(),
                source,
            },
        )
    })
}

pub(in crate::openraft) fn decode_v1<T: DeserializeOwned>(
    key: RaftRecordKey,
    bytes: &[u8],
) -> Result<T, std::io::Error> {
    serde_json::from_slice(bytes).map_err(|source| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            PersistenceCodecError {
                operation: "decode",
                key_class: key.class(),
                source,
            },
        )
    })
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde::Serialize;

    use crate::typ::EntryPayload;
    use crate::typ::LogEntry;
    use crate::typ::LogId;
    use crate::typ::StoredMembership;
    use crate::typ::Vote;

    use super::decode_v1;
    use super::encode_v1;
    use super::RaftRecordKey;

    #[derive(Debug, Deserialize, PartialEq, Serialize)]
    struct GoldenRecord {
        generation: u64,
        active: bool,
    }

    #[test]
    fn v1_codec_keeps_the_existing_compact_json_bytes() {
        let record = GoldenRecord {
            generation: 7,
            active: true,
        };

        let bytes = encode_v1(RaftRecordKey::ReplicasInfoManagerState, &record).expect("encode fixture");
        assert_eq!(bytes, br#"{"generation":7,"active":true}"#);
        assert_eq!(
            decode_v1::<GoldenRecord>(RaftRecordKey::ReplicasInfoManagerState, &bytes,).expect("decode fixture"),
            record
        );
    }

    #[test]
    fn invalid_v1_bytes_report_only_the_record_class() {
        let error = decode_v1::<GoldenRecord>(RaftRecordKey::LastMembership, b"{secret-invalid-json")
            .expect_err("invalid fixture");

        assert!(error.to_string().contains("membership"));
        assert!(!error.to_string().contains("secret-invalid-json"));
    }

    #[test]
    fn raft_v1_domain_records_keep_their_existing_json_bytes() {
        let vote = Vote::new(2, 1);
        let log_id = LogId {
            leader_id: Vote::new(1, 1).leader_id,
            index: 7,
        };
        let entry = LogEntry {
            log_id,
            payload: EntryPayload::Blank,
        };
        let membership = StoredMembership::default();

        assert_eq!(
            String::from_utf8(encode_v1(RaftRecordKey::Vote, &vote).expect("vote")).expect("UTF-8 vote"),
            r#"{"leader_id":{"term":2,"node_id":1},"committed":false}"#
        );
        assert_eq!(
            String::from_utf8(encode_v1(RaftRecordKey::LastPurgedLog, &log_id).expect("log id"),)
                .expect("UTF-8 log id"),
            r#"{"leader_id":{"term":1,"node_id":1},"index":7}"#
        );
        assert_eq!(
            String::from_utf8(encode_v1(RaftRecordKey::LogEntry(7), &entry).expect("log entry"),)
                .expect("UTF-8 log entry"),
            r#"{"log_id":{"leader_id":{"term":1,"node_id":1},"index":7},"payload":"Blank"}"#
        );
        assert_eq!(
            String::from_utf8(encode_v1(RaftRecordKey::LastMembership, &membership).expect("membership"),)
                .expect("UTF-8 membership"),
            r#"{"log_id":null,"membership":{"configs":[],"nodes":{}}}"#
        );
    }
}
