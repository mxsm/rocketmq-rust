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

use rocketmq_protocol::protocol::header::notification_response_header::NotificationResponseHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;

pub(super) fn compose_notification_response(
    command_factory: &RemotingCommandFactory,
    has_msg: bool,
    polling_full: bool,
    opaque: i32,
) -> RemotingCommand {
    command_factory
        .create_success_response_command_with_header(NotificationResponseHeader { has_msg, polling_full })
        .set_opaque(opaque)
}

#[cfg(test)]
mod tests {
    use rocketmq_protocol::protocol::header::notification_response_header::NotificationResponseHeader;
    use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;

    use super::*;

    #[test]
    fn notification_wake_response_composer_preserves_opaque_and_header() {
        let mut response = compose_notification_response(&application_remoting_command_factory(), false, false, 9833);
        response.make_custom_header_to_net();
        let header = response
            .decode_command_custom_header::<NotificationResponseHeader>()
            .expect("Notification response header");

        assert_eq!(response.opaque(), 9833);
        assert!(!header.has_msg);
        assert!(!header.polling_full);
        assert!(response.body().is_none());

        let mut full = compose_notification_response(&application_remoting_command_factory(), false, true, 9834);
        full.make_custom_header_to_net();
        let full_header = full
            .decode_command_custom_header::<NotificationResponseHeader>()
            .expect("Notification polling-full header");
        assert!(!full_header.has_msg);
        assert!(full_header.polling_full);
    }
}
