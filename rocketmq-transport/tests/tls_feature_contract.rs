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

use rocketmq_transport::config::TlsConfig;
use rocketmq_transport::config::TlsMode;
use rocketmq_transport::tls::TlsServerRuntime;

#[test]
fn tls_runtime_preserves_default_permissive_mode() {
    let runtime = TlsServerRuntime::new(TlsConfig::default());
    assert_eq!(runtime.mode(), TlsMode::Permissive);
    runtime.shutdown();
}

#[cfg(not(feature = "tls"))]
#[test]
fn tls_off_reports_a_typed_disabled_error() {
    let error = rocketmq_transport::tls::tls_disabled_error();
    assert!(error.to_string().contains("compiled without the tls feature"));
}
