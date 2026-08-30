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

/// Versioned API transport contracts.
pub mod v1 {
    use schemars::JsonSchema;
    use serde::Deserialize;
    use serde::Serialize;

    /// Bounded page used by Phase 2 read APIs.
    #[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
    pub struct ApiPage<T> {
        pub items: Vec<T>,
        pub next_cursor: Option<String>,
        pub partial: bool,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        pub warnings: Vec<String>,
    }

    /// Read-only operations frozen into the Phase 2 public contract.
    #[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum ReadOnlyOperation {
        ReadAlerts,
        ReadTopology,
        ReadSloHealth,
        ReadForecasts,
        RunSimulation,
        ReadReadiness,
        ManagePostmortemMetadata,
        ManageActionItemMetadata,
    }

    /// Capability marker returned alongside the Phase 2 API schema.
    #[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
    pub struct Phase2ContractManifest {
        pub schema_version: String,
        pub effective_access: String,
        pub cluster_mutation_supported: bool,
        pub operations: Vec<ReadOnlyOperation>,
    }

    impl Default for Phase2ContractManifest {
        fn default() -> Self {
            Self {
                schema_version: "rocketmq-sre.api.v1".into(),
                effective_access: "read_only".into(),
                cluster_mutation_supported: false,
                operations: vec![
                    ReadOnlyOperation::ReadAlerts,
                    ReadOnlyOperation::ReadTopology,
                    ReadOnlyOperation::ReadSloHealth,
                    ReadOnlyOperation::ReadForecasts,
                    ReadOnlyOperation::RunSimulation,
                    ReadOnlyOperation::ReadReadiness,
                    ReadOnlyOperation::ManagePostmortemMetadata,
                    ReadOnlyOperation::ManageActionItemMetadata,
                ],
            }
        }
    }
}
