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

#![recursion_limit = "256"]

use rocketmq_client_rust::ClientConfig;

#[test]
fn client_config_parses_proxy_json_once_into_the_transport_model() {
    let mut client = ClientConfig::default();
    client.set_socks_proxy_config(
        r#"{"0.0.0.0/0":{"addr":"127.0.0.1:1080","username":"alice","password":"secret"}}"#.into(),
    );

    let parsed = client.parse_socks_proxy_config().expect("valid proxy config");
    assert!(!parsed.is_empty());
    assert_eq!(
        parsed
            .route_for("10.0.0.1", Some("10.0.0.1".parse().unwrap()))
            .expect("default IPv4 route")
            .endpoint(),
        "127.0.0.1:1080"
    );

    client.set_socks_proxy_config(r#"{"0.0.0.0/0":{"addr":"127.0.0.1:1080","password":"secret"}}"#.into());
    let error = client
        .parse_socks_proxy_config()
        .expect_err("partial authentication must be rejected before transport startup");
    assert!(error.to_string().contains("username and password"));
    assert!(!error.to_string().contains("secret"));
}
