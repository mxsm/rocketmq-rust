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

/// Reasons a Broker rejects a consumer's pull operation.
///
/// Numeric values match Apache RocketMQ's wire protocol and must remain unchanged.
pub struct ForbiddenType;
impl ForbiddenType {
    /// The Broker does not permit reading messages.
    pub const BROKER_FORBIDDEN: i32 = 1;
    /// The subscription group does not permit consumption.
    pub const GROUP_FORBIDDEN: i32 = 2;
    /// The topic does not permit reading messages.
    pub const TOPIC_FORBIDDEN: i32 = 3;
    /// Broadcasting consumption is disabled for the subscription group.
    pub const BROADCASTING_DISABLE_FORBIDDEN: i32 = 4;
    /// Consumption is forbidden for this subscription group's topic.
    pub const SUBSCRIPTION_FORBIDDEN: i32 = 5;
}
