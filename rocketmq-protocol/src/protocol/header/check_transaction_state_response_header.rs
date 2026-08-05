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

use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Default, Deserialize, Serialize, RequestHeaderCodecV2)]
#[request_header(validate = "validate")]
#[serde(rename_all = "camelCase")]
pub struct CheckTransactionStateResponseHeader {
    #[required]
    pub producer_group: CheetahString,
    #[required]
    pub tran_state_table_offset: i64,
    #[required]
    pub commit_log_offset: i64,
    #[required]
    pub commit_or_rollback: i32,
}

impl CheckTransactionStateResponseHeader {
    fn validate(&self) -> rocketmq_error::RocketMQResult<()> {
        use rocketmq_model::common::sys_flag::message_sys_flag::MessageSysFlag;

        match self.commit_or_rollback {
            MessageSysFlag::TRANSACTION_COMMIT_TYPE | MessageSysFlag::TRANSACTION_ROLLBACK_TYPE => Ok(()),
            _ => Err(rocketmq_error::RocketMQError::request_header_error(
                "CheckTransactionStateResponseHeader.commitOrRollback: expected commit or rollback",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::FromMap;

    fn map_with_state(state: i32) -> HashMap<CheetahString, CheetahString> {
        HashMap::from([
            ("producerGroup".into(), "group-a".into()),
            ("tranStateTableOffset".into(), "1".into()),
            ("commitLogOffset".into(), "2".into()),
            ("commitOrRollback".into(), state.to_string().into()),
        ])
    }

    #[test]
    fn accepts_commit_and_rollback_only() {
        use rocketmq_model::common::sys_flag::message_sys_flag::MessageSysFlag;

        for state in [
            MessageSysFlag::TRANSACTION_COMMIT_TYPE,
            MessageSysFlag::TRANSACTION_ROLLBACK_TYPE,
        ] {
            assert!(<CheckTransactionStateResponseHeader as FromMap>::from(&map_with_state(state)).is_ok());
        }
        assert!(<CheckTransactionStateResponseHeader as FromMap>::from(&map_with_state(
            MessageSysFlag::TRANSACTION_NOT_TYPE
        ))
        .is_err());
    }
}
