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

use super::*;
use crate::admin::{NameserverReadOperation, NameserverReadReply, NameserverReplyStatus};

impl MQClientAPIImpl {
    pub(crate) async fn read_nameserver_observation_at(
        &self,
        endpoint: CheetahString,
        operation: NameserverReadOperation,
        timeout_millis: u64,
        max_body_bytes: usize,
    ) -> ClientResult<NameserverReadReply> {
        if endpoint.is_empty() || timeout_millis == 0 || max_body_bytes == 0 || max_body_bytes > 16 * 1024 * 1024 {
            return Err(mq_client_err!("invalid bounded nameserver observation"));
        }
        let request = match operation {
            NameserverReadOperation::ViewConfiguration => self.create_remoting_command(RequestCode::GetNamesrvConfig),
            NameserverReadOperation::TopicRoute { topic } => {
                if topic.is_empty() || topic.len() > 255 || topic.chars().any(char::is_control) {
                    return Err(mq_client_err!("invalid observation Topic"));
                }
                self.create_request_command(
                    RequestCode::GetRouteinfoByTopic,
                    GetRouteInfoRequestHeader {
                        topic: CheetahString::from(topic),
                        accept_standard_json_only: Some(true),
                        topic_request_header: None,
                    },
                )
            }
        };
        let outcome = self
            .remoting_client
            .invoke_request_with_deadline(
                Some(&endpoint),
                request,
                RequestDeadline::from_timeout_millis(timeout_millis),
            )
            .await
            .map_err(|error| ClientError::from_shared(error.into_shared_error()))?;
        let mut response = match outcome {
            OutboundRequestResult::Response(response) => response,
            OutboundRequestResult::Rejected(rejection) => {
                return Err(route_error::route_lookup_error(RetryInput::Rejected(rejection)))
            }
            OutboundRequestResult::Contract(contract) => {
                return Err(route_error::route_lookup_error(RetryInput::Contract(contract)))
            }
        };
        let code = ResponseCode::from(response.code());
        let body = response.take_body();
        let bytes = body.as_ref().map_or(0, |body| body.len());
        let status = if bytes > max_body_bytes {
            NameserverReplyStatus::TooLarge
        } else {
            match code {
                ResponseCode::Success if body.is_some() => NameserverReplyStatus::Success,
                ResponseCode::TopicNotExist => NameserverReplyStatus::TopicAbsent,
                ResponseCode::NoPermission => NameserverReplyStatus::PermissionDenied,
                ResponseCode::RequestCodeNotSupported => NameserverReplyStatus::Unsupported,
                ResponseCode::Success => NameserverReplyStatus::Invalid,
                _ => NameserverReplyStatus::Unavailable,
            }
        };
        let retained = if status == NameserverReplyStatus::Success {
            body.map_or_else(Vec::new, |body| body.to_vec())
        } else {
            Vec::new()
        };
        Ok(NameserverReadReply::new(status, bytes, retained))
    }
}
