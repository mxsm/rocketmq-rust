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

#![cfg(feature = "cluster-mode")]

use rocketmq_proxy::ProxyConfig;

#[path = "../../scripts/tests/rust/core_helm_configs.rs"]
mod core_helm_configs;

#[test]
#[ignore = "requires CORE_HELM_CONFIG_DIR exported by scripts/core_helm_configs.py"]
fn rendered_core_helm_configs_use_proxy_loader() {
    for path in core_helm_configs::rendered_configs("proxy") {
        let config = ProxyConfig::load_from_file(&path).unwrap();
        config.grpc.socket_addr().unwrap();
        config.grpc.tls.validate().unwrap();
        assert!(config.grpc.tls.enabled, "{path:?}");
        assert!(!config.remoting.enabled);
        assert!(config.cluster.namesrv_addr.as_ref().unwrap().contains(".svc."));
        assert_eq!(config.grpc.tls.certificate_path, "/etc/rocketmq/tls/tls.crt");
        assert_eq!(config.grpc.tls.private_key_path, "/etc/rocketmq/tls/tls.key");
        assert!(config.cluster.control_reserve < config.cluster.io_max_inflight);
    }
}
