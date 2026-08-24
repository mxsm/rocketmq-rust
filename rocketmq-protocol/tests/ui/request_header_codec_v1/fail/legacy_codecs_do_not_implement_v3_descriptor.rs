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

use rocketmq_macros::{RequestHeaderCodec, RequestHeaderCodecV2};
use rocketmq_protocol::protocol::header_codec::{HeaderCodec, HeaderFieldSpec};

pub mod protocol {
    pub mod command_custom_header {
        pub use rocketmq_protocol::{CommandCustomHeader, FromMap};
    }
}

#[allow(
    deprecated,
    reason = "proves RequestHeaderCodec V1 does not gain a V3 descriptor contract"
)]
mod legacy_v1_header {
    use super::*;

    #[derive(RequestHeaderCodec)]
    pub struct LegacyV1Header {
        value: i32,
    }
}

use legacy_v1_header::LegacyV1Header;

#[allow(
    deprecated,
    reason = "proves RequestHeaderCodecV2 does not gain a V3 descriptor contract"
)]
mod legacy_v2_header {
    use super::*;

    #[derive(RequestHeaderCodecV2)]
    #[request_header_codec_v2(crate = "rocketmq_protocol")]
    pub struct LegacyV2Header {
        value: i32,
    }
}

use legacy_v2_header::LegacyV2Header;

#[allow(
    deprecated,
    reason = "proves silent RequestHeaderCodec V1 tuple expansion produces no legacy codec traits"
)]
mod silent_tuple_header {
    use super::*;

    #[derive(RequestHeaderCodec)]
    pub struct SilentTupleHeader(i32);
}

use silent_tuple_header::SilentTupleHeader;

#[allow(
    deprecated,
    reason = "proves silent RequestHeaderCodec V1 unit expansion produces no legacy codec traits"
)]
mod silent_unit_header {
    use super::*;

    #[derive(RequestHeaderCodec)]
    pub struct SilentUnitHeader;
}

use silent_unit_header::SilentUnitHeader;

#[allow(
    deprecated,
    reason = "proves silent RequestHeaderCodec V1 enum expansion produces no legacy codec traits"
)]
mod silent_enum_header {
    use super::*;

    #[derive(RequestHeaderCodec)]
    pub enum SilentEnumHeader {
        V1,
    }
}

use silent_enum_header::SilentEnumHeader;

trait LocalV3Descriptor {
    const TYPE_ID: &'static str;
    const LOCAL_FIELD_SPECS: &'static [HeaderFieldSpec];

    fn visit_field_specs(visitor: &mut dyn FnMut(&HeaderFieldSpec));
}

#[diagnostic::do_not_recommend]
impl<T: HeaderCodec> LocalV3Descriptor for T {
    const TYPE_ID: &'static str = T::TYPE_ID;
    const LOCAL_FIELD_SPECS: &'static [HeaderFieldSpec] = T::LOCAL_FIELD_SPECS;

    fn visit_field_specs(visitor: &mut dyn FnMut(&HeaderFieldSpec)) {
        T::visit_field_specs(visitor);
    }
}

fn requires_v3_descriptor<T: LocalV3Descriptor>() {
    let _ = T::TYPE_ID;
    let _ = T::LOCAL_FIELD_SPECS;
    T::visit_field_specs(&mut |_| {});
}

trait LocalLegacyCommandCustomHeader {}

#[diagnostic::do_not_recommend]
impl<T: crate::protocol::command_custom_header::CommandCustomHeader> LocalLegacyCommandCustomHeader for T {}

trait LocalLegacyFromMap {}

#[diagnostic::do_not_recommend]
impl<T: crate::protocol::command_custom_header::FromMap> LocalLegacyFromMap for T {}

fn requires_legacy_command_custom_header<T: LocalLegacyCommandCustomHeader>() {}

fn requires_legacy_from_map<T: LocalLegacyFromMap>() {}

fn main() {
    requires_v3_descriptor::<LegacyV1Header>();
    requires_v3_descriptor::<LegacyV2Header>();
    requires_legacy_command_custom_header::<SilentTupleHeader>();
    requires_legacy_command_custom_header::<SilentUnitHeader>();
    requires_legacy_command_custom_header::<SilentEnumHeader>();
    requires_legacy_from_map::<SilentTupleHeader>();
    requires_legacy_from_map::<SilentUnitHeader>();
    requires_legacy_from_map::<SilentEnumHeader>();
}
