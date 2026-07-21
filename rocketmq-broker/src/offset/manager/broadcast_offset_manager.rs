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

use tracing::warn;

#[derive(Debug, Default)]
pub struct BroadcastOffsetManager {}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct BroadcastOffsetCapability;

#[allow(unused_variables)]
impl BroadcastOffsetCapability {
    pub(crate) fn query_init_offset(
        &self,
        topic: &str,
        group_id: &str,
        queue_id: i32,
        client_id: &str,
        request_offset: i64,
        from_proxy: bool,
    ) -> i64 {
        BroadcastOffsetManager::default().query_init_offset(
            topic,
            group_id,
            queue_id,
            client_id,
            request_offset,
            from_proxy,
        )
    }

    pub(crate) fn update_offset(
        &self,
        topic: &str,
        group: &str,
        queue_id: i32,
        offset: i64,
        client_id: &str,
        from_proxy: bool,
    ) {
        BroadcastOffsetManager::default().update_offset(topic, group, queue_id, offset, client_id, from_proxy);
    }
}

#[allow(unused_variables)]
impl BroadcastOffsetManager {
    pub(crate) fn pull_capability(&self) -> BroadcastOffsetCapability {
        BroadcastOffsetCapability
    }

    pub fn start(&mut self) {
        warn!("BroadcastOffsetManager started is not implemented");
    }

    pub fn query_init_offset(
        &self,
        topic: &str,
        group_id: &str,
        queue_id: i32,
        client_id: &str,
        request_offset: i64,
        from_proxy: bool,
    ) -> i64 {
        unimplemented!()
    }

    pub fn update_offset(
        &self,
        topic: &str,
        group: &str,
        queue_id: i32,
        offset: i64,
        client_id: &str,
        from_proxy: bool,
    ) {
        unimplemented!()
    }

    pub fn shutdown(&mut self) {
        warn!("BroadcastOffsetManager shutdown is not implemented");
    }
}
