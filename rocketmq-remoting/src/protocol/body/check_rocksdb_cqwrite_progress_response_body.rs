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

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(i32)]
pub enum CheckStatus {
    CheckOk = 0,
    CheckNotOk = 1,
    CheckInProgress = 2,
    CheckError = 3,
}

impl From<i32> for CheckStatus {
    fn from(value: i32) -> Self {
        match value {
            0 => CheckStatus::CheckOk,
            1 => CheckStatus::CheckNotOk,
            2 => CheckStatus::CheckInProgress,
            3 => CheckStatus::CheckError,
            _ => CheckStatus::CheckError,
        }
    }
}

/// Result of checking RocksDB consume queue write progress.
/// Corresponds to Java's `CheckRocksdbCqWriteResult`.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CheckRocksdbCqWriteResult {
    pub check_result: Option<CheetahString>,
    pub check_status: i32,
}

impl CheckRocksdbCqWriteResult {
    pub fn get_check_status(&self) -> CheckStatus {
        CheckStatus::from(self.check_status)
    }
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn check_rocksdb_cq_write_result_default_values() {
        let result = CheckRocksdbCqWriteResult::default();
        assert!(result.check_result.is_none());
        assert_eq!(result.check_status, 0);
    }

    #[test]
    fn check_rocksdb_cq_write_result_with_values() {
        let result = CheckRocksdbCqWriteResult {
            check_result: Some(CheetahString::from("all topic is ready")),
            check_status: 0,
        };
        assert_eq!(result.check_result, Some(CheetahString::from("all topic is ready")));
        assert_eq!(result.get_check_status(), CheckStatus::CheckOk);
    }

    #[test]
    fn serialize_check_rocksdb_cq_write_result() {
        let result = CheckRocksdbCqWriteResult {
            check_result: Some(CheetahString::from("check doing")),
            check_status: 2,
        };
        let serialized = serde_json::to_string(&result).unwrap();
        assert!(serialized.contains("\"checkResult\":\"check doing\""));
        assert!(serialized.contains("\"checkStatus\":2"));
    }

    #[test]
    fn deserialize_check_rocksdb_cq_write_result() {
        let json = r#"{"checkResult":"all ok","checkStatus":0}"#;
        let result: CheckRocksdbCqWriteResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.check_result, Some(CheetahString::from("all ok")));
        assert_eq!(result.check_status, 0);
        assert_eq!(result.get_check_status(), CheckStatus::CheckOk);
    }

    #[test]
    fn deserialize_check_rocksdb_cq_write_result_error() {
        let json = r#"{"checkResult":"error info","checkStatus":3}"#;
        let result: CheckRocksdbCqWriteResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.get_check_status(), CheckStatus::CheckError);
    }

    #[test]
    fn check_status_from_i32() {
        assert_eq!(CheckStatus::from(0), CheckStatus::CheckOk);
        assert_eq!(CheckStatus::from(1), CheckStatus::CheckNotOk);
        assert_eq!(CheckStatus::from(2), CheckStatus::CheckInProgress);
        assert_eq!(CheckStatus::from(3), CheckStatus::CheckError);
        assert_eq!(CheckStatus::from(99), CheckStatus::CheckError);
    }
}
