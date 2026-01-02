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

pub mod call_snapshot;
pub mod moment_stats_item;
pub mod moment_stats_item_set;
pub mod stats_item;
pub mod stats_item_set;
pub mod stats_snapshot;

pub struct Stats;

impl Stats {
    pub const BROKER_GET_FROM_DISK_NUMS: &'static str = "BROKER_GET_FROM_DISK_NUMS";
    pub const BROKER_GET_FROM_DISK_SIZE: &'static str = "BROKER_GET_FROM_DISK_SIZE";
    pub const BROKER_GET_NUMS: &'static str = "BROKER_GET_NUMS";
    pub const BROKER_PUT_NUMS: &'static str = "BROKER_PUT_NUMS";
    pub const COMMERCIAL_PERM_FAILURES: &'static str = "COMMERCIAL_PERM_FAILURES";
    pub const COMMERCIAL_RCV_EPOLLS: &'static str = "COMMERCIAL_RCV_EPOLLS";
    pub const COMMERCIAL_RCV_SIZE: &'static str = "COMMERCIAL_RCV_SIZE";
    pub const COMMERCIAL_RCV_TIMES: &'static str = "COMMERCIAL_RCV_TIMES";
    pub const COMMERCIAL_SEND_SIZE: &'static str = "COMMERCIAL_SEND_SIZE";
    pub const COMMERCIAL_SEND_TIMES: &'static str = "COMMERCIAL_SEND_TIMES";
    pub const COMMERCIAL_SNDBCK_TIMES: &'static str = "COMMERCIAL_SNDBCK_TIMES";
    pub const GROUP_GET_FALL_SIZE: &'static str = "GROUP_GET_FALL_SIZE";
    pub const GROUP_GET_FALL_TIME: &'static str = "GROUP_GET_FALL_TIME";
    pub const GROUP_GET_FROM_DISK_NUMS: &'static str = "GROUP_GET_FROM_DISK_NUMS";
    pub const GROUP_GET_FROM_DISK_SIZE: &'static str = "GROUP_GET_FROM_DISK_SIZE";
    pub const GROUP_GET_LATENCY: &'static str = "GROUP_GET_LATENCY";
    pub const GROUP_GET_NUMS: &'static str = "GROUP_GET_NUMS";
    pub const GROUP_GET_SIZE: &'static str = "GROUP_GET_SIZE";
    pub const QUEUE_GET_NUMS: &'static str = "QUEUE_GET_NUMS";
    pub const QUEUE_GET_SIZE: &'static str = "QUEUE_GET_SIZE";
    pub const QUEUE_PUT_NUMS: &'static str = "QUEUE_PUT_NUMS";
    pub const QUEUE_PUT_SIZE: &'static str = "QUEUE_PUT_SIZE";
    pub const SNDBCK_PUT_NUMS: &'static str = "SNDBCK_PUT_NUMS";
    pub const TOPIC_PUT_NUMS: &'static str = "TOPIC_PUT_NUMS";
    pub const TOPIC_PUT_SIZE: &'static str = "TOPIC_PUT_SIZE";
}
