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

use std::fmt;
use std::str::FromStr;

#[derive(PartialEq, Default, Debug, Copy, Clone)]
pub enum CQType {
    #[default]
    SimpleCQ,
    BatchCQ,
    RocksDBCQ,
}

impl fmt::Display for CQType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            CQType::SimpleCQ => write!(f, "SimpleCQ"),
            CQType::BatchCQ => write!(f, "BatchCQ"),
            CQType::RocksDBCQ => write!(f, "RocksDBCQ"),
        }
    }
}

impl FromStr for CQType {
    type Err = crate::ModelContractViolation;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "SIMPLECQ" => Ok(CQType::SimpleCQ),
            "BATCHCQ" => Ok(CQType::BatchCQ),
            "ROCKSDBCQ" => Ok(CQType::RocksDBCQ),
            _ => Err(crate::ModelContractViolation::InvalidCqType),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_str_simplecq() {
        let result = CQType::from_str("simplecq");
        assert_eq!(result.unwrap(), CQType::SimpleCQ);
    }

    #[test]
    fn test_from_str_batchcq() {
        let result = CQType::from_str("batchcq");
        assert_eq!(result.unwrap(), CQType::BatchCQ);
    }

    #[test]
    fn test_from_str_rocksdbcq() {
        let result = CQType::from_str("rocksdbcq");
        assert_eq!(result.unwrap(), CQType::RocksDBCQ);
    }

    #[test]
    fn test_from_str_invalid() {
        let rejected_value = "untrusted-cq-type";
        assert_eq!(
            CQType::from_str(rejected_value),
            Err(crate::ModelContractViolation::InvalidCqType)
        );
        let error = CQType::from_str(rejected_value).expect_err("invalid CQ type should be rejected");
        assert_eq!(error.to_string(), "consume queue type is invalid");
        assert_eq!(format!("{error:?}"), "InvalidCqType");
        assert!(!format!("{error:?}").contains(rejected_value));
    }
}
