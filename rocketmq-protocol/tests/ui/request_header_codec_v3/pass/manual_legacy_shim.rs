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

use rocketmq_macros::RequestHeaderCodecV3;
use rocketmq_protocol::protocol::header_codec::HeaderCodec;
use rocketmq_protocol::{CommandCustomHeader, FromMap, HeaderMap};

#[derive(RequestHeaderCodecV3)]
#[header(
    type_id = "fixtures::ManualLegacyShim",
    legacy_shim = "manual",
    crate = "rocketmq_protocol"
)]
struct ManualLegacyShim {
    #[header(required)]
    value: i32,
}

impl CommandCustomHeader for ManualLegacyShim {
    fn to_map(&self) -> Option<HeaderMap> {
        let mut map = HeaderMap::new();
        self.try_encode_into_map(&mut map).ok()?;
        Some(map)
    }

    fn try_encode_into_map(
        &self,
        out: &mut HeaderMap,
    ) -> Result<(), rocketmq_protocol::ProtocolContractViolation> {
        let mut sink = rocketmq_protocol::protocol::header_codec::MapSink::new(out);
        <Self as HeaderCodec>::encode_into(self, &mut sink)
    }
}

impl FromMap for ManualLegacyShim {
    type Error = rocketmq_protocol::__request_header_codec::RocketMQError;
    type Target = Self;

    fn from(map: &HeaderMap) -> Result<Self::Target, Self::Error> {
        <Self as HeaderCodec>::decode_from_map(map)
            .map_err(rocketmq_protocol::protocol::header_codec::into_rocketmq_error)
    }
}

fn main() {}
