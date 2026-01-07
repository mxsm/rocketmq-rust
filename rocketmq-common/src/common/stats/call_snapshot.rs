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

#[derive(Debug, PartialEq, Copy, Clone)]
pub struct CallSnapshot {
    timestamp: u64,
    times: u64,
    value: u64,
}

impl CallSnapshot {
    pub fn new(timestamp: u64, times: u64, value: u64) -> Self {
        CallSnapshot {
            timestamp,
            times,
            value,
        }
    }

    // Getter for timestamp
    pub fn get_timestamp(&self) -> u64 {
        self.timestamp
    }

    // Getter for times
    pub fn get_times(&self) -> u64 {
        self.times
    }

    // Getter for value
    pub fn get_value(&self) -> u64 {
        self.value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn call_snapshot_initializes_correctly() {
        let snapshot = CallSnapshot::new(100, 200, 300);
        assert_eq!(snapshot.get_timestamp(), 100);
        assert_eq!(snapshot.get_times(), 200);
        assert_eq!(snapshot.get_value(), 300);
    }
}
