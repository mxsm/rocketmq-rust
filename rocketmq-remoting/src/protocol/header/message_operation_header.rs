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

pub mod send_message_request_header;
pub mod send_message_request_header_v2;
pub mod send_message_response_header;

pub trait TopicRequestHeaderTrait: Sync + Send {
    fn set_lo(&mut self, lo: Option<bool>);

    fn lo(&self) -> Option<bool>;

    fn set_topic(&mut self, topic: CheetahString);

    fn topic(&self) -> &CheetahString;

    fn broker_name(&self) -> Option<&CheetahString>;

    fn set_broker_name(&mut self, broker_name: CheetahString);

    fn namespace(&self) -> Option<&str>;

    fn set_namespace(&mut self, namespace: CheetahString);

    fn namespaced(&self) -> Option<bool>;

    fn set_namespaced(&mut self, namespaced: bool);

    fn oneway(&self) -> Option<bool>;

    fn set_oneway(&mut self, oneway: bool);

    fn queue_id(&self) -> i32;

    fn set_queue_id(&mut self, queue_id: i32);
}
