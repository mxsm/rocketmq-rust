/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::str::FromStr;

use anyhow::anyhow;

#[derive(PartialEq, Default, Debug)]
pub enum CQType {
    #[default]
    SimpleCQ,
    BatchCQ,
    RocksDBCQ,
}

impl FromStr for CQType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "SIMPLECQ" => Ok(CQType::SimpleCQ),
            "BATCHCQ" => Ok(CQType::BatchCQ),
            "ROCKSDBCQ" => Ok(CQType::RocksDBCQ),
            _ => Err(anyhow!("Parse from string error,Invalid CQType: {}", s)),
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
        let result = CQType::from_str("invalidcq");
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "Parse from string error,Invalid CQType: invalidcq"
        );
    }
}
