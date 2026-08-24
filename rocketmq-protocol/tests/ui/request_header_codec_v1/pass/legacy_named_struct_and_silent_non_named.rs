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

use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodec;
use serde::{Deserialize, Serialize};

// RequestHeaderCodec V1 expands these paths relative to its consuming crate.
pub mod protocol {
    pub mod command_custom_header {
        pub use rocketmq_protocol::{CommandCustomHeader, FromMap};
    }
}

#[allow(
    deprecated,
    reason = "proves the supported named RequestHeaderCodec V1 expansion remains source-compatible"
)]
mod supported_named_header {
    use super::*;

    #[derive(Serialize, Deserialize, RequestHeaderCodec)]
    pub struct SupportedNamedHeader {
        #[required]
        request_id: CheetahString,
        attempt_count: Option<i32>,
    }
}

use supported_named_header::SupportedNamedHeader;

#[allow(
    deprecated,
    reason = "freezes the historical silent RequestHeaderCodec V1 tuple expansion"
)]
mod silent_tuple_header {
    use super::*;

    #[derive(RequestHeaderCodec)]
    pub struct SilentTupleHeader(pub String);
}

use silent_tuple_header::SilentTupleHeader;

#[allow(
    deprecated,
    reason = "freezes the historical silent RequestHeaderCodec V1 unit expansion"
)]
mod silent_unit_header {
    use super::*;

    #[derive(RequestHeaderCodec)]
    pub struct SilentUnitHeader;
}

use silent_unit_header::SilentUnitHeader;

#[allow(
    deprecated,
    reason = "freezes the historical silent RequestHeaderCodec V1 enum expansion"
)]
mod silent_enum_header {
    use super::*;

    #[derive(RequestHeaderCodec)]
    pub enum SilentEnumHeader {
        V1,
    }
}

use silent_enum_header::SilentEnumHeader;

fn requires_legacy_header<T>()
where
    T: crate::protocol::command_custom_header::CommandCustomHeader + crate::protocol::command_custom_header::FromMap,
{
}

fn main() {
    requires_legacy_header::<SupportedNamedHeader>();
    let _ = SilentTupleHeader("V1 silently expands unsupported non-named shapes to nothing".to_owned());
    let _ = SilentUnitHeader;
    let _ = SilentEnumHeader::V1;
}
