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

use std::time::Instant;

use crate::code::request_code::RequestCode;
use crate::code::response_code::ResponseCode;
use crate::protocol::remoting_command::RemotingCommand;

const NO_RESPONSE_CODE: i32 = -1;
const RESULT_ONEWAY: &str = "oneway";
const RESULT_SUCCESS: &str = "success";
const RESULT_CANCELED: &str = "cancelled";
const RESULT_PROCESS_REQUEST_FAILED: &str = "process_request_failed";
const RESULT_WRITE_CHANNEL_FAILED: &str = "write_channel_failed";

pub(crate) struct RequestMetricsGuard {
    start: Instant,
    request_code: i32,
    is_long_polling: bool,
    rpc_recorded: bool,
}

impl RequestMetricsGuard {
    #[inline]
    pub(crate) fn start(request: &RemotingCommand, request_bytes: u64) -> Self {
        rocketmq_observability::metrics::remoting::record_requests_total(1);
        record_network_bytes(request_bytes);

        Self {
            start: Instant::now(),
            request_code: request.code(),
            is_long_polling: is_long_polling_request(request.code()),
            rpc_recorded: false,
        }
    }

    #[inline]
    pub(crate) fn complete_response(&mut self, response_code: i32) {
        self.record_rpc_latency(response_code, RESULT_SUCCESS);
    }

    #[inline]
    pub(crate) fn complete_oneway(&mut self) {
        self.record_rpc_latency(NO_RESPONSE_CODE, RESULT_ONEWAY);
    }

    #[inline]
    pub(crate) fn complete_cancelled(&mut self) {
        self.record_rpc_latency(NO_RESPONSE_CODE, RESULT_CANCELED);
    }

    #[inline]
    pub(crate) fn complete_process_request_failed(&mut self) {
        self.record_rpc_latency(ResponseCode::SystemError.to_i32(), RESULT_PROCESS_REQUEST_FAILED);
    }

    #[inline]
    pub(crate) fn complete_write_channel_failed(&mut self, response_code: i32) {
        self.record_rpc_latency(response_code, RESULT_WRITE_CHANNEL_FAILED);
    }

    #[inline]
    fn record_rpc_latency(&mut self, response_code: i32, result: &str) {
        if self.rpc_recorded {
            return;
        }
        rocketmq_observability::metrics::remoting::record_rpc_latency(
            self.start.elapsed().as_millis() as u64,
            self.request_code,
            response_code,
            self.is_long_polling,
            result,
        );
        self.rpc_recorded = true;
    }
}

impl Drop for RequestMetricsGuard {
    fn drop(&mut self) {
        rocketmq_observability::metrics::remoting::record_request_latency(self.start.elapsed().as_millis() as u64);
        if !self.rpc_recorded {
            self.complete_cancelled();
        }
    }
}

#[inline]
pub(crate) fn record_network_bytes(bytes: u64) {
    if bytes > 0 {
        rocketmq_observability::metrics::remoting::record_network_bytes(bytes);
    }
}

#[inline]
fn is_long_polling_request(request_code: i32) -> bool {
    matches!(
        RequestCode::from(request_code),
        RequestCode::PullMessage
            | RequestCode::PopMessage
            | RequestCode::PopLiteMessage
            | RequestCode::LitePullMessage
            | RequestCode::Notification
            | RequestCode::PollingInfo
    )
}

#[cfg(test)]
mod tests {
    use crate::code::request_code::RequestCode;

    use super::*;

    #[test]
    fn long_polling_request_codes_match_broker_poll_paths() {
        assert!(is_long_polling_request(RequestCode::PullMessage.to_i32()));
        assert!(is_long_polling_request(RequestCode::PopMessage.to_i32()));
        assert!(is_long_polling_request(RequestCode::Notification.to_i32()));
        assert!(!is_long_polling_request(RequestCode::SendMessage.to_i32()));
    }
}
