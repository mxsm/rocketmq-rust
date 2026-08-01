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

use std::fs;
use std::path::Path;

#[test]
fn control_plane_has_no_connector_or_rocketmq_client_dependency() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let manifest = fs::read_to_string(root.join("Cargo.toml")).expect("control-plane manifest");
    for forbidden in ["rocketmq-admin-core", "rocketmq-mcp", "rocketmq-sre-connector"] {
        assert!(
            !manifest.contains(forbidden),
            "control plane must use ConnectorChannel instead of {forbidden}"
        );
    }
}
