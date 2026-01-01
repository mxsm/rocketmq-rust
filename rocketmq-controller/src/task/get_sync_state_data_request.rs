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
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetSyncStateDataRequest {
    invoke_time: u64,
}

impl GetSyncStateDataRequest {
    pub fn new() -> Self {
        Self {
            invoke_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
        }
    }

    pub fn invoke_time(&self) -> u64 {
        self.invoke_time
    }
}

impl Default for GetSyncStateDataRequest {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for GetSyncStateDataRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GetSyncStateDataRequest{{invokeTime={}}}", self.invoke_time)
    }
}
