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

use std::collections::HashMap;

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryCorrectionOffsetBody {
    pub correction_offsets: HashMap<i32, i64>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::protocol::RemotingDeserializable;
    use crate::protocol::RemotingSerializable;

    use super::QueryCorrectionOffsetBody;

    #[test]
    fn query_correction_offset_body_round_trips() {
        let body = QueryCorrectionOffsetBody {
            correction_offsets: HashMap::from([(0, 10), (1, i64::MAX)]),
        };

        let encoded = body.encode().expect("encode query correction offset body");
        let decoded =
            QueryCorrectionOffsetBody::decode(encoded.as_slice()).expect("decode query correction offset body");
        assert_eq!(decoded.correction_offsets.get(&0), Some(&10));
        assert_eq!(decoded.correction_offsets.get(&1), Some(&i64::MAX));
    }
}
