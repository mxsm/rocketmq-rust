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

pub struct FAQUrl;

impl FAQUrl {
    pub const APPLY_TOPIC_URL: &'static str = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";
    pub const CLIENT_PARAMETER_CHECK_URL: &'static str = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";
    pub const CLIENT_SERVICE_NOT_OK: &'static str = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";
    pub const GROUP_NAME_DUPLICATE_URL: &'static str = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";
    pub const LOAD_JSON_EXCEPTION: &'static str = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";
    const MORE_INFORMATION: &'static str = "For more information, please visit the url, ";
    pub const MQLIST_NOT_EXIST: &'static str = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";
    pub const NAME_SERVER_ADDR_NOT_EXIST_URL: &'static str = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";
    // FAQ: No route info of this topic, TopicABC
    pub const NO_TOPIC_ROUTE_INFO: &'static str = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";
    pub const SAME_GROUP_DIFFERENT_TOPIC: &'static str = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";
    pub const SEND_MSG_FAILED: &'static str = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";
    pub const SUBSCRIPTION_GROUP_NOT_EXIST: &'static str = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";
    const TIP_STRING_BEGIN: &'static str = "\nSee ";
    const TIP_STRING_END: &'static str = " for further details.";
    pub const UNEXPECTED_EXCEPTION_URL: &'static str = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";
    pub const UNKNOWN_HOST_EXCEPTION: &'static str = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

    pub fn suggest_todo(url: &'static str) -> String {
        format!("{}{}{}", Self::TIP_STRING_BEGIN, url, Self::TIP_STRING_END)
    }

    pub fn attach_default_url(error_message: Option<&str>) -> String {
        if let Some(err_msg) = error_message {
            if !err_msg.contains(Self::TIP_STRING_BEGIN) {
                return format!(
                    "{}\n{}{}",
                    err_msg,
                    Self::MORE_INFORMATION,
                    Self::UNEXPECTED_EXCEPTION_URL
                );
            }
        }
        error_message.unwrap_or_default().to_string()
    }
}
