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

use rocketmq_broker::config::raw::RawBrokerConfig;
use rocketmq_broker::config::validated::ValidatedBrokerConfig;

#[path = "../../scripts/tests/rust/core_helm_configs.rs"]
mod core_helm_configs;

#[test]
#[ignore = "requires CORE_HELM_CONFIG_DIR exported by scripts/core_helm_configs.py"]
fn rendered_core_helm_configs_use_broker_loader() {
    for path in core_helm_configs::rendered_configs("broker") {
        let raw = RawBrokerConfig::load(&path).unwrap();
        let config = ValidatedBrokerConfig::try_from(raw).unwrap();
        assert!(config.broker().broker_ip1.contains(".svc."), "{path:?}");
        assert!(config.broker().namesrv_addr.as_ref().unwrap().contains(".svc."));
        assert_eq!(
            config.broker().enable_controller_mode,
            config.store().enable_controller_mode
        );
        assert_eq!(config.broker().store_path_root_dir, config.store().store_path_root_dir);
        assert!(config.store().min_in_sync_replicas <= config.store().total_replicas);
    }
}
