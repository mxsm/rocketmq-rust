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

pub(super) fn map_producer_connection(
    topic: &str,
    producer_group: &str,
    connection: &ProducerConnection,
) -> dashboard::DashboardProducerConnections {
    let mut connections = connection
        .connection_set()
        .iter()
        .map(|item| dashboard::DashboardProducerConnection {
            client_id: item.get_client_id().to_string(),
            client_addr: item.get_client_addr().to_string(),
            language: item.get_language().to_string(),
            version: item.get_version(),
            version_desc: value2version(item.get_version().max(0) as u32).name().to_string(),
        })
        .collect::<Vec<_>>();
    connections.sort_by(|left, right| left.client_id.cmp(&right.client_id));
    dashboard::DashboardProducerConnections {
        topic: topic.to_string(),
        producer_group: producer_group.to_string(),
        connections,
    }
}
