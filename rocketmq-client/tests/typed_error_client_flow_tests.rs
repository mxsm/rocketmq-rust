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
fn client_callback_files_do_not_use_legacy_error_enum() {
    let files = [
        include_str!("../src/consumer/consumer_impl/default_mq_push_consumer_impl.rs"),
        include_str!("../src/consumer/pop_callback.rs"),
        include_str!("../src/consumer/pull_callback.rs"),
        include_str!("../src/producer/producer_impl/default_mq_producer_impl.rs"),
    ];

    for source in files {
        assert!(!source.contains(concat!("Rocket", "mqError")));
        assert!(!source.contains(concat!("RemotingTooMuchRequest", "Error")));
        assert!(!source.contains(concat!("MQ", "ClientErr(Client", "Err")));
        assert!(!source.contains(concat!("downcast_ref::<Rocket", "mqError>")));
    }
}

#[test]
fn client_public_api_does_not_export_dead_error_module() {
    let lib = include_str!("../src/lib.rs");

    assert!(!lib.contains(concat!("pub mod client", "_error;")));
}

#[test]
fn client_callback_error_paths_use_typed_rocketmq_error() {
    let ack_callback = include_str!("../src/consumer/ack_callback.rs");
    let pull_callback = include_str!("../src/consumer/pull_callback.rs");
    let pop_callback = include_str!("../src/consumer/pop_callback.rs");
    let request_callback = include_str!("../src/producer/request_callback.rs");
    let request_future = include_str!("../src/producer/request_response_future.rs");
    let send_callback = include_str!("../src/producer/send_callback.rs");

    assert!(ack_callback.contains("fn on_exception(&self, e: RocketMQError)"));
    assert!(ack_callback.contains("Result<(), RocketMQError>"));
    assert!(!ack_callback.contains("Box<dyn std::error::Error"));

    assert!(pull_callback.contains("fn on_exception(&mut self, e: RocketMQError)"));
    assert!(pull_callback.contains("fn broker_response_code(error: &RocketMQError)"));
    assert!(!pull_callback.contains("downcast_ref::<RocketMQError>"));
    assert!(!pull_callback.contains("Box<dyn std::error::Error + Send>"));

    assert!(pop_callback.contains("fn on_error(&mut self, e: RocketMQError)"));
    assert!(pop_callback.contains("fn broker_response_code(error: &RocketMQError)"));
    assert!(!pop_callback.contains("downcast_ref::<RocketMQError>"));
    assert!(!pop_callback.contains("Box<dyn std::error::Error + Send>"));

    assert!(request_callback.contains("Option<&RocketMQError>"));
    assert!(request_future.contains("type RequestCause = Arc<RocketMQError>"));
    assert!(!request_future.contains("type RequestCause = Arc<dyn"));

    assert!(send_callback.contains("fn on_exception(&self, error: &RocketMQError)"));
    assert!(send_callback.contains("Option<&RocketMQError>"));
    assert!(!send_callback.contains("Option<&dyn std::error::Error>"));
}

#[test]
fn client_hook_contexts_store_typed_errors() {
    let send_message_context = include_str!("../src/hook/send_message_context.rs");
    let check_forbidden_context = include_str!("../src/hook/check_forbidden_context.rs");

    assert!(send_message_context.contains("pub exception: Option<Arc<RocketMQError>>"));
    assert!(check_forbidden_context.contains("pub exception: Option<RocketMQError>"));
    assert!(!send_message_context.contains("Box<dyn Error + Send + Sync>"));
    assert!(!check_forbidden_context.contains("Box<dyn std::error::Error + Send + Sync>"));
}
