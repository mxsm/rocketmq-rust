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

#[test]
fn error_crate_public_api_exposes_only_typed_error_surface() {
    let source = include_str!("../src/lib.rs");
    let removed_symbols = [
        concat!("Legacy", "RocketMQResult"),
        concat!("pub enum Rocket", "mqError"),
        concat!("pub struct MQBroker", "Err"),
        concat!("pub struct Client", "Err"),
        concat!("pub struct RequestTimeout", "Err"),
        concat!("macro_rules! mq_client_err", "_legacy"),
        concat!("pub enum Legacy", "ServiceError"),
    ];

    for symbol in removed_symbols {
        assert!(
            !source.contains(symbol),
            "`rocketmq-error` should not expose legacy error surface symbol `{symbol}`"
        );
    }
}
