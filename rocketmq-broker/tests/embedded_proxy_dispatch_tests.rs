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

use rocketmq_transport::RequestContext;
use rocketmq_transport::RequestContextError;

const PROXY_FACADE_SOURCE: &str = include_str!("../src/proxy_facade.rs");
const PIPELINE_STARTUP_SOURCE: &str = include_str!("../src/broker_runtime/request_pipeline/startup.rs");

#[test]
fn embedded_proxy_production_path_uses_the_shared_in_process_dispatcher() {
    for forbidden in ["LocalRequestHarness", "TcpListener", "TcpStream", "127.0.0.1:0"] {
        assert!(
            !PROXY_FACADE_SOURCE.contains(forbidden),
            "production ProxyFacade must not contain loopback transport primitive {forbidden}"
        );
    }
    for required in [
        "authorized_dispatcher()",
        "RequestContext::try_embedded",
        "Principal::new(\"embedded-proxy\")",
        "dispatch_embedded",
        "RequestDeadline::after",
    ] {
        assert!(
            PROXY_FACADE_SOURCE.contains(required),
            "production ProxyFacade must retain shared dispatch invariant {required}"
        );
    }
    assert!(
        PIPELINE_STARTUP_SOURCE.contains("AuthorizedCommandDispatcher::try_new"),
        "the Broker composition root must construct the shared dispatcher"
    );
    assert_eq!(
        PIPELINE_STARTUP_SOURCE.matches("with_authorized_dispatcher").count(),
        2,
        "normal and fast remoting servers must receive the same Broker dispatcher"
    );
}

#[test]
fn embedded_request_context_fails_closed_without_a_trusted_identity() {
    assert_eq!(
        RequestContext::try_embedded(None, None).expect_err("missing principal must be rejected"),
        RequestContextError::MissingEmbeddedPrincipal
    );
}
