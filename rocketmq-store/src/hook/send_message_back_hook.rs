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

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_ext::MessageExt;

pub trait SendMessageBackHook {
    fn execute_send_message_back(
        &self,
        msg_list: &mut [MessageExt],
        broker_name: &CheetahString,
        broker_addr: &CheetahString,
    ) -> bool;
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use rocketmq_common::common::message::message_ext::MessageExt;

    use super::*;

    struct MockSendMessageBackHook;

    impl SendMessageBackHook for MockSendMessageBackHook {
        fn execute_send_message_back(
            &self,
            msg_list: &mut [MessageExt],
            broker_name: &CheetahString,
            broker_addr: &CheetahString,
        ) -> bool {
            !msg_list.is_empty() && !broker_name.is_empty() && !broker_addr.is_empty()
        }
    }

    #[test]
    fn execute_send_message_back_success() {
        let hook = MockSendMessageBackHook;
        let mut msg_list = vec![MessageExt::default()];
        let broker_name = CheetahString::from("broker1");
        let broker_addr = CheetahString::from("127.0.0.1:10911");
        assert!(hook.execute_send_message_back(&mut msg_list, &broker_name, &broker_addr));
    }

    #[test]
    fn execute_send_message_back_empty_message_list() {
        let hook = MockSendMessageBackHook;
        let mut msg_list = vec![];
        let broker_name = CheetahString::from("broker1");
        let broker_addr = CheetahString::from("127.0.0.1:10911");
        assert!(!hook.execute_send_message_back(&mut msg_list, &broker_name, &broker_addr));
    }

    #[test]
    fn execute_send_message_back_empty_broker_name() {
        let hook = MockSendMessageBackHook;
        let mut msg_list = vec![MessageExt::default()];
        let broker_name = CheetahString::new();
        let broker_addr = CheetahString::from("127.0.0.1:10911");
        assert!(!hook.execute_send_message_back(&mut msg_list, &broker_name, &broker_addr));
    }

    #[test]
    fn execute_send_message_back_empty_broker_addr() {
        let hook = MockSendMessageBackHook;
        let mut msg_list = vec![MessageExt::default()];
        let broker_name = CheetahString::from("broker1");
        let broker_addr = CheetahString::new();
        assert!(!hook.execute_send_message_back(&mut msg_list, &broker_name, &broker_addr));
    }
}
