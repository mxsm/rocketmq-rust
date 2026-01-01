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

pub struct ClientErrorCode;

impl ClientErrorCode {
    pub const CONNECT_BROKER_EXCEPTION: i32 = 10001;
    pub const ACCESS_BROKER_TIMEOUT: i32 = 10002;
    pub const BROKER_NOT_EXIST_EXCEPTION: i32 = 10003;
    pub const NO_NAME_SERVER_EXCEPTION: i32 = 10004;
    pub const NOT_FOUND_TOPIC_EXCEPTION: i32 = 10005;
    pub const REQUEST_TIMEOUT_EXCEPTION: i32 = 10006;
    pub const CREATE_REPLY_MESSAGE_EXCEPTION: i32 = 10007;
}
