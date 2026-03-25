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

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct NameServerStatusItem {
    pub(crate) address: String,
    pub(crate) is_current: bool,
    pub(crate) is_alive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct NameServerHomePageView {
    pub(crate) current_namesrv: Option<String>,
    pub(crate) namesrv_addr_list: Vec<String>,
    #[serde(rename = "useVIPChannel")]
    pub(crate) use_vip_channel: bool,
    #[serde(rename = "useTLS")]
    pub(crate) use_tls: bool,
    pub(crate) servers: Vec<NameServerStatusItem>,
}
