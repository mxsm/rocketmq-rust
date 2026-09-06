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

use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::header_codec::{HeaderFieldContext, HeaderValue, HeaderValueKind};
use rocketmq_protocol::ProtocolContractViolation;

struct ExternalValue;

impl HeaderValue for ExternalValue {
    const KIND: HeaderValueKind = HeaderValueKind::String;

    fn to_map_value(&self) -> CheetahString {
        CheetahString::new()
    }

    fn encoded_len(&self) -> usize {
        0
    }

    fn write_ascii(&self, _out: &mut BytesMut) {}

    fn decode(_raw: &str, _context: HeaderFieldContext) -> Result<Self, ProtocolContractViolation> {
        Ok(Self)
    }
}

fn main() {}
