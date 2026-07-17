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

use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;

const LOCAL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct LocalConfig {
    pub broker_cluster_name: String,
    pub broker_name: String,
    pub broker_ip: String,
    pub broker_listen_port: u16,
    pub store_root_dir: String,
}

impl LocalConfig {
    pub fn shutdown_timeout(&self) -> Duration {
        LOCAL_SHUTDOWN_TIMEOUT
    }
}

impl Default for LocalConfig {
    fn default() -> Self {
        Self {
            broker_cluster_name: "DefaultCluster".to_owned(),
            broker_name: "rocketmq-proxy-local".to_owned(),
            broker_ip: "127.0.0.1".to_owned(),
            broker_listen_port: 10911,
            store_root_dir: "store/proxy/local-broker".to_owned(),
        }
    }
}
