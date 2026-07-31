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

use std::collections::HashMap;
use std::panic::catch_unwind;
use std::panic::AssertUnwindSafe;

use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::header::get_max_offset_request_header::GetMaxOffsetRequestHeader;
use rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

#[test]
#[ignore = "red reproducer: remote pull headers must never reach an unwrap panic"]
fn pull_header_decode_unwrap_does_not_panic_for_untrusted_fields() {
    let mut malformed = valid_pull_fields();
    malformed.insert(field("queueId"), field("not-a-number"));
    let mut overflow = valid_pull_fields();
    overflow.insert(field("queueId"), field("2147483648"));
    let cases = [
        ("missing ext fields", RemotingCommand::create_remoting_command(0)),
        (
            "missing required fields",
            RemotingCommand::create_remoting_command(0).set_ext_fields(HashMap::new()),
        ),
        (
            "malformed numeric field",
            RemotingCommand::create_remoting_command(0).set_ext_fields(malformed),
        ),
        (
            "overflowing numeric field",
            RemotingCommand::create_remoting_command(0).set_ext_fields(overflow),
        ),
    ];

    let panics = cases
        .into_iter()
        .filter_map(|(case, command)| {
            catch_unwind(AssertUnwindSafe(|| {
                let _ = command
                    .decode_command_custom_header_fast::<PullMessageRequestHeader>()
                    .unwrap();
            }))
            .is_err()
            .then_some(case)
        })
        .collect::<Vec<_>>();

    assert!(
        panics.is_empty(),
        "remote pull header decoding panicked for: {panics:?}"
    );
}

#[test]
#[ignore = "red reproducer: remote admin headers must never reach an unwrap panic"]
fn admin_header_decode_unwrap_does_not_panic_for_untrusted_fields() {
    let mut malformed = valid_admin_fields();
    malformed.insert(field("queueId"), field("not-a-number"));
    let mut overflow = valid_admin_fields();
    overflow.insert(field("queueId"), field("2147483648"));
    let cases = [
        ("missing ext fields", RemotingCommand::create_remoting_command(0)),
        (
            "malformed numeric field",
            RemotingCommand::create_remoting_command(0).set_ext_fields(malformed),
        ),
        (
            "overflowing numeric field",
            RemotingCommand::create_remoting_command(0).set_ext_fields(overflow),
        ),
    ];

    let panics = cases
        .into_iter()
        .filter_map(|(case, command)| {
            catch_unwind(AssertUnwindSafe(|| {
                let _ = command
                    .decode_command_custom_header::<GetMaxOffsetRequestHeader>()
                    .unwrap();
            }))
            .is_err()
            .then_some(case)
        })
        .collect::<Vec<_>>();

    assert!(
        panics.is_empty(),
        "remote admin header decoding panicked for: {panics:?}"
    );
}

fn valid_pull_fields() -> HashMap<CheetahString, CheetahString> {
    [
        ("consumerGroup", "consumer"),
        ("topic", "topic"),
        ("queueId", "0"),
        ("queueOffset", "0"),
        ("maxMsgNums", "32"),
        ("sysFlag", "0"),
        ("commitOffset", "0"),
        ("suspendTimeoutMillis", "1000"),
        ("subVersion", "0"),
    ]
    .into_iter()
    .map(|(key, value)| (field(key), field(value)))
    .collect()
}

fn valid_admin_fields() -> HashMap<CheetahString, CheetahString> {
    [("topic", "topic"), ("queueId", "0"), ("committed", "false")]
        .into_iter()
        .map(|(key, value)| (field(key), field(value)))
        .collect()
}

fn field(value: &str) -> CheetahString {
    CheetahString::from(value)
}
