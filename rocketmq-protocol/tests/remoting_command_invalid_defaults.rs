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

use rocketmq_protocol::protocol::remoting_command::SERIALIZE_TYPE_PROPERTY;
use rocketmq_protocol::protocol::remoting_command_facade::initialize_remoting_defaults;
use rocketmq_protocol::protocol::remoting_command_facade::RemotingDefaultsError;

#[test]
fn invalid_process_configuration_fails_before_defaults_are_initialized() {
    // SAFETY: this integration-test binary contains one test, so no concurrent thread can read or
    // mutate this process environment key while the assertion runs.
    unsafe {
        std::env::set_var(SERIALIZE_TYPE_PROPERTY, "CBOR");
    }

    let error = initialize_remoting_defaults(4242).expect_err("invalid serialization config must fail startup");
    let RemotingDefaultsError::InvalidSerializeType(error) = error else {
        panic!("expected invalid serialization type error");
    };
    assert_eq!(error.key(), SERIALIZE_TYPE_PROPERTY);
    assert_eq!(error.value(), "CBOR");
}
