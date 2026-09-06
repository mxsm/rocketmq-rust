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

use rocketmq_proxy::ProxyConfig;
use rocketmq_proxy::ProxyRuntime;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;

fn main() {
    let owner = RuntimeOwner::plan(RuntimeConfig::server_default("compile-fail")).expect("test runtime configuration is valid").build().unwrap();
    let _ = ProxyRuntime::builder(
        ProxyConfig::default(),
        owner.root_context(),
        rocketmq_observability::TelemetryHandle::noop(),
    );
}
