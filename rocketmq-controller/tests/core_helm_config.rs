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

#[path = "../../scripts/tests/rust/core_helm_configs.rs"]
mod core_helm_configs;

#[test]
#[ignore = "requires CORE_HELM_CONFIG_DIR exported by scripts/core_helm_configs.py"]
fn rendered_core_helm_configs_use_controller_loader() {
    for path in core_helm_configs::rendered_configs("controller") {
        let config: rocketmq_controller::ControllerConfig =
            rocketmq_runtime::common::parse_config_file::parse_config_file(path.clone()).unwrap();
        config.validate().unwrap();
        assert!(config.local_raft_addr().ip().is_unspecified(), "{path:?}");
        assert_eq!(config.raft_member_endpoints().len(), 3);
        assert!(config
            .controller_endpoint_for(config.node_id)
            .unwrap()
            .contains(".svc."));
        assert!(config.storage_path.starts_with("/var/lib/rocketmq/"));
        let properties = config.to_properties_string();
        assert!(properties.contains("raftPeerEndpoints=1-core-controller-0."));
    }
}
