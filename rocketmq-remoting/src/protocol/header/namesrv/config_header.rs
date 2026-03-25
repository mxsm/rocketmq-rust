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

use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Default, Deserialize, Serialize, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct GetNamesrvConfigRequestHeader {
    pub probe_only: Option<bool>,
}

impl GetNamesrvConfigRequestHeader {
    pub fn for_probe() -> Self {
        Self { probe_only: Some(true) }
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::GetNamesrvConfigRequestHeader;
    use crate::code::request_code::RequestCode;
    use crate::protocol::remoting_command::RemotingCommand;

    #[test]
    fn for_probe_sets_probe_only_flag() {
        let header = GetNamesrvConfigRequestHeader::for_probe();
        assert_eq!(header.probe_only, Some(true));
    }

    #[test]
    fn request_command_encodes_probe_only_header() {
        let mut command = RemotingCommand::create_request_command(
            RequestCode::GetNamesrvConfig,
            GetNamesrvConfigRequestHeader::for_probe(),
        );
        command.make_custom_header_to_net();

        let ext_fields = command.ext_fields().expect("header fields should exist");
        assert_eq!(ext_fields.get("probeOnly").map(CheetahString::as_str), Some("true"));
    }

    #[test]
    fn request_command_decodes_probe_only_header() {
        let mut command = RemotingCommand::create_request_command(
            RequestCode::GetNamesrvConfig,
            GetNamesrvConfigRequestHeader::for_probe(),
        );
        command.make_custom_header_to_net();

        let decoded = command
            .decode_command_custom_header_fast::<GetNamesrvConfigRequestHeader>()
            .expect("header should decode");
        assert_eq!(decoded.probe_only, Some(true));
    }
}
