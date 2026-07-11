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

pub mod allocate_message_queue_averagely;
pub mod allocate_message_queue_averagely_by_circle;
pub mod allocate_message_queue_by_config;
pub mod allocate_message_queue_by_machine_room;
pub mod allocate_message_queue_by_machine_room_nearby;
pub mod allocate_message_queue_consistent_hash;

pub use rocketmq_model::allocation::check;
