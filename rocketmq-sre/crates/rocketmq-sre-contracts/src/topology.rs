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

use std::collections::BTreeMap;

use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::AssetKind;
use crate::ClusterId;
use crate::TenantId;
use crate::TopologyEdge;
use crate::TopologySnapshotId;

/// One normalized node in a point-in-time topology snapshot.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct TopologyNode {
    pub key: String,
    pub kind: AssetKind,
    pub display_name: String,
    pub source: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    pub partial: bool,
}

/// Immutable, bounded topology projection used by correlation and UI.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct TopologySnapshot {
    pub id: TopologySnapshotId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub nodes: Vec<TopologyNode>,
    pub edges: Vec<TopologyEdge>,
    pub observed_at: DateTime<Utc>,
    pub freshness_seconds: u64,
    pub partial: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    pub content_hash: String,
}
