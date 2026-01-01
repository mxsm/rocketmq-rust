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

use crate::consumer::ack_status::AckStatus;

#[derive(Debug, Clone, Default)]
pub struct AckResult {
    pub(crate) status: AckStatus,
    pub(crate) extra_info: CheetahString,
    pub(crate) pop_time: i64,
}

impl std::fmt::Display for AckResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AckResult [AckStatus={:?}, extraInfo={}]",
            self.status, self.extra_info
        )
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn ack_result_display_format() {
        let ack_result = AckResult {
            status: AckStatus::Ok,
            extra_info: CheetahString::from("extra info"),
            pop_time: 123456789,
        };
        assert_eq!(
            format!("{}", ack_result),
            "AckResult [AckStatus=Ok, extraInfo=extra info]"
        );
    }

    #[test]
    fn ack_result_display_format_with_not_exist_status() {
        let ack_result = AckResult {
            status: AckStatus::NotExist,
            extra_info: CheetahString::from("extra info"),
            pop_time: 987654321,
        };
        assert_eq!(
            format!("{}", ack_result),
            "AckResult [AckStatus=NotExist, extraInfo=extra info]"
        );
    }

    #[test]
    fn ack_result_display_format_with_empty_extra_info() {
        let ack_result = AckResult {
            status: AckStatus::Ok,
            extra_info: CheetahString::from(""),
            pop_time: 123456789,
        };
        assert_eq!(format!("{}", ack_result), "AckResult [AckStatus=Ok, extraInfo=]");
    }

    #[test]
    fn ack_result_display_format_with_negative_pop_time() {
        let ack_result = AckResult {
            status: AckStatus::Ok,
            extra_info: CheetahString::from("extra info"),
            pop_time: -123456789,
        };
        assert_eq!(
            format!("{}", ack_result),
            "AckResult [AckStatus=Ok, extraInfo=extra info]"
        );
    }
}
