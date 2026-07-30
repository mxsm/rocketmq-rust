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

use crate::proto::v2;

pub(super) fn receive_message(response: &v2::ReceiveMessageResponse) -> Option<&v2::Status> {
    match response.content.as_ref() {
        Some(v2::receive_message_response::Content::Status(status)) => Some(status),
        _ => None,
    }
}

pub(super) fn pull_message(response: &v2::PullMessageResponse) -> Option<&v2::Status> {
    match response.content.as_ref() {
        Some(v2::pull_message_response::Content::Status(status)) => Some(status),
        _ => None,
    }
}
