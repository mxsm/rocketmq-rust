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
use rocketmq_model::utils::string_utils::cheetah_from_integer;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::common::time_utils::current_millis;

use crate::latency::broker_fast_failure::FastFailureQueueKind;

pub(crate) mod ack_message_processor;
pub(crate) mod admin_broker_processor;
mod born_host;
pub(crate) mod change_invisible_time_processor;
pub(crate) mod client_manage_processor;
pub(crate) mod consumer_manage_processor;
pub(crate) mod default_pull_message_result_handler;
pub(crate) mod dispatcher;
pub(crate) mod end_transaction_processor;
mod fast_failure_dispatch;
pub(crate) mod lite_manager_processor;
pub(crate) mod lite_subscription_ctl_processor;
pub(crate) mod maintenance_request_processor;
pub(crate) mod notification_processor;
pub(crate) mod peek_message_processor;
pub(crate) mod polling_info_processor;
pub(crate) mod pop_inflight_message_counter;
pub(crate) mod pop_lite_message_processor;
pub(crate) mod pop_message_processor;
pub(crate) mod processor_service;
#[cfg(test)]
#[path = "../tests/unit/processor_test_support.rs"]
pub(crate) mod processor_test_support;
pub(crate) mod pull_message_processor;
pub(crate) mod pull_message_result_handler;
pub(crate) mod query_assignment_processor;
pub(crate) mod query_message_processor;
pub(crate) mod recall_message_processor;
pub(crate) mod reply_message_processor;
mod request_ordering;
pub(crate) mod response_assembly;
pub(crate) mod send_message_processor;

const BORN_TIME: &str = "bornTime";

fn normalize_born_time(request: &mut RemotingCommand) {
    if matches!(request.ext_field(BORN_TIME), None | Some("0")) {
        request.add_ext_field(
            CheetahString::from_static_str(BORN_TIME),
            cheetah_from_integer(current_millis()),
        );
    }
}

const fn is_privileged_maintenance_request(request_code: RequestCode) -> bool {
    matches!(
        request_code,
        RequestCode::MaintenanceGetCapabilities
            | RequestCode::MaintenanceCreateStoreCheckpoint
            | RequestCode::MaintenanceVerifyCheckpoint
            | RequestCode::MaintenanceRestoreVerify
    )
}
fn fast_failure_queue_kind(request_code: i32, default_processor: bool) -> Option<FastFailureQueueKind> {
    if default_processor {
        return Some(FastFailureQueueKind::AdminBroker);
    }

    match RequestCode::from(request_code) {
        RequestCode::SendMessage
        | RequestCode::SendMessageV2
        | RequestCode::SendBatchMessage
        | RequestCode::ConsumerSendMsgBack => Some(FastFailureQueueKind::Send),
        RequestCode::PullMessage => Some(FastFailureQueueKind::Pull),
        RequestCode::LitePullMessage => Some(FastFailureQueueKind::LitePull),
        RequestCode::HeartBeat => Some(FastFailureQueueKind::Heartbeat),
        RequestCode::EndTransaction => Some(FastFailureQueueKind::Transaction),
        RequestCode::AckMessage | RequestCode::BatchAckMessage => Some(FastFailureQueueKind::Ack),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_only_missing_and_zero_born_time() {
        for input in [None, Some("0"), Some("12345"), Some("00"), Some(""), Some("invalid")] {
            let mut request = RemotingCommand::create_remoting_command(RequestCode::PopMessage.to_i32());
            if let Some(value) = input {
                request.add_ext_field("bornTime", value);
            }

            normalize_born_time(&mut request);

            let actual = request.ext_field("bornTime").unwrap();
            match input {
                None | Some("0") => assert!(actual.parse::<u64>().unwrap() > 0),
                Some(value) => assert_eq!(actual, value),
            }
        }
    }
}
