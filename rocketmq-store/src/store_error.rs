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
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("RocksDB error: {0}")]
    RocksDb(String),

    #[error("Store is not started")]
    NotStarted,

    #[error("Message not found")]
    MessageNotFound,

    #[error("General store error: {0}")]
    General(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rocksdb_error() {
        let error = StoreError::RocksDb("Database error".to_string());
        assert_eq!(format!("{}", error), "RocksDB error: Database error");
    }

    #[test]
    fn store_not_started() {
        let error = StoreError::NotStarted;
        assert_eq!(format!("{}", error), "Store is not started");
    }

    #[test]
    fn message_not_found() {
        let error = StoreError::MessageNotFound;
        assert_eq!(format!("{}", error), "Message not found");
    }

    #[test]
    fn general_store_error() {
        let error = StoreError::General("An error occurred".to_string());
        assert_eq!(
            format!("{}", error),
            "General store error: An error occurred"
        );
    }
}
