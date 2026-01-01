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

use std::fmt::Display;

use rocketmq_common::common::message::message_ext::MessageExt;

use crate::consumer::pop_status::PopStatus;

#[derive(Default, Clone)]
pub struct PopResult {
    pub msg_found_list: Option<Vec<MessageExt>>,
    pub pop_status: PopStatus,
    pub pop_time: u64,
    pub invisible_time: u64,
    pub rest_num: u64,
}

impl Display for PopResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PopResult [msg_found_list={}, pop_status={}, pop_time={}, invisible_time={}, rest_num={}]",
            self.msg_found_list.as_ref().map_or(0, |value| value.len()),
            self.pop_status,
            self.pop_time,
            self.invisible_time,
            self.rest_num
        )
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_common::common::message::message_ext::MessageExt;

    use super::*;
    use crate::consumer::pop_status::PopStatus;

    fn create_message_ext() -> MessageExt {
        MessageExt::default()
    }

    #[test]
    fn display_pop_result_with_empty_msg_list() {
        let pop_result = PopResult {
            msg_found_list: Some(vec![]),
            pop_status: PopStatus::Found,
            pop_time: 123456789,
            invisible_time: 1000,
            rest_num: 10,
        };
        assert_eq!(
            format!("{}", pop_result),
            "PopResult [msg_found_list=0, pop_status=FOUND, pop_time=123456789, invisible_time=1000, rest_num=10]"
        );
    }

    #[test]
    fn display_pop_result_with_non_empty_msg_list() {
        let pop_result = PopResult {
            msg_found_list: Some(vec![create_message_ext(), create_message_ext()]),
            pop_status: PopStatus::NoNewMsg,
            pop_time: 987654321,
            invisible_time: 2000,
            rest_num: 5,
        };
        assert_eq!(
            format!("{}", pop_result),
            "PopResult [msg_found_list=2, pop_status=NO_NEW_MSG, pop_time=987654321, invisible_time=2000, rest_num=5]"
        );
    }

    #[test]
    fn display_pop_result_with_polling_full_status() {
        let pop_result = PopResult {
            msg_found_list: Some(vec![create_message_ext()]),
            pop_status: PopStatus::PollingFull,
            pop_time: 111111111,
            invisible_time: 3000,
            rest_num: 0,
        };
        assert_eq!(
            format!("{}", pop_result),
            "PopResult [msg_found_list=1, pop_status=POLLING_FULL, pop_time=111111111, invisible_time=3000, \
             rest_num=0]"
        );
    }

    #[test]
    fn display_pop_result_with_polling_not_found_status() {
        let pop_result = PopResult {
            msg_found_list: Some(vec![]),
            pop_status: PopStatus::PollingNotFound,
            pop_time: 222222222,
            invisible_time: 4000,
            rest_num: 20,
        };
        assert_eq!(
            format!("{}", pop_result),
            "PopResult [msg_found_list=0, pop_status=POLLING_NOT_FOUND, pop_time=222222222, invisible_time=4000, \
             rest_num=20]"
        );
    }
}
