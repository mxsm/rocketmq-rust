// Copyright 2026 The RocketMQ Rust Authors
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

use std::collections::HashMap;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmptyHeader {}

impl CommandCustomHeader for EmptyHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        Some(HashMap::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_empty_header_lifecycle() {
        let header = EmptyHeader::default();

        let json = serde_json::to_string(&header).unwrap();
        assert_eq!(json, "{}");
        let de: EmptyHeader = serde_json::from_str(&json).unwrap();
        assert_eq!(header, de);
        assert_eq!(header.to_map(), Some(HashMap::new()));
    }
}
