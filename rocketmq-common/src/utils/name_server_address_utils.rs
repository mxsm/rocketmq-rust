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

use std::env;

use crate::common::mix_all;

const INSTANCE_PREFIX: &str = "MQ_INST_";

fn is_word_char(value: char) -> bool {
    value.is_ascii_alphanumeric() || value == '_'
}

pub struct NameServerAddressUtils;

impl NameServerAddressUtils {
    pub fn get_name_server_addresses() -> Option<String> {
        env::var(mix_all::NAMESRV_ADDR_PROPERTY)
            .or_else(|_| env::var(mix_all::NAMESRV_ADDR_ENV))
            .ok()
    }

    pub fn is_name_srv_endpoint(name_srv_addr: &str) -> bool {
        name_srv_addr.starts_with("http://")
    }

    pub fn validate_instance_endpoint(endpoint: &str) -> bool {
        let endpoint_without_scheme = if let Some((scheme, rest)) = endpoint.split_once("://") {
            if scheme.is_empty() || !scheme.chars().all(is_word_char) {
                return false;
            }
            rest
        } else {
            endpoint
        };

        let Some(instance_part) = endpoint_without_scheme.strip_prefix(INSTANCE_PREFIX) else {
            return false;
        };
        let word_prefix_len = instance_part
            .find(|value: char| !is_word_char(value))
            .unwrap_or(instance_part.len());
        let word_prefix = &instance_part[..word_prefix_len];
        let Some(separator_index) = word_prefix.find('_') else {
            return false;
        };
        separator_index > 0 && separator_index + 1 < word_prefix.len()
    }

    pub fn parse_instance_id_from_endpoint(endpoint: &str) -> Option<String> {
        if endpoint.is_empty() {
            None
        } else {
            let start = endpoint.rfind('/').map_or(0, |index| index + 1);
            let dot_pos = endpoint.find('.').unwrap_or(endpoint.len());
            Some(endpoint[start..dot_pos].to_string())
        }
    }

    pub fn get_name_srv_addr_from_namesrv_endpoint(name_srv_endpoint: &str) -> Option<String> {
        if name_srv_endpoint.is_empty() {
            None
        } else {
            let start = name_srv_endpoint.rfind('/').map_or(0, |index| index + 1);
            Some(name_srv_endpoint[start..].to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_instance_endpoint_matches_java_prefix_shape() {
        assert!(NameServerAddressUtils::validate_instance_endpoint("MQ_INST_abc_def"));
        assert!(NameServerAddressUtils::validate_instance_endpoint(
            "http://MQ_INST_abc_def.example.com"
        ));
        assert!(NameServerAddressUtils::validate_instance_endpoint(
            "ons://MQ_INST_abc_def"
        ));
        assert!(!NameServerAddressUtils::validate_instance_endpoint("http://invalid"));
        assert!(!NameServerAddressUtils::validate_instance_endpoint(
            "http://MQ_INST_abc_"
        ));
        assert!(!NameServerAddressUtils::validate_instance_endpoint(
            "bad-scheme://MQ_INST_abc_def"
        ));
    }

    #[test]
    fn parse_instance_id_keeps_first_char_when_endpoint_has_no_slash() {
        assert_eq!(
            NameServerAddressUtils::parse_instance_id_from_endpoint("MQ_INST_abc_def.example.com").as_deref(),
            Some("MQ_INST_abc_def")
        );
        assert_eq!(
            NameServerAddressUtils::parse_instance_id_from_endpoint("http://MQ_INST_abc_def.example.com").as_deref(),
            Some("MQ_INST_abc_def")
        );
        assert_eq!(NameServerAddressUtils::parse_instance_id_from_endpoint(""), None);
    }

    #[test]
    fn get_name_srv_addr_keeps_first_char_when_endpoint_has_no_slash() {
        assert_eq!(
            NameServerAddressUtils::get_name_srv_addr_from_namesrv_endpoint("127.0.0.1:9876").as_deref(),
            Some("127.0.0.1:9876")
        );
        assert_eq!(
            NameServerAddressUtils::get_name_srv_addr_from_namesrv_endpoint("http://127.0.0.1:9876").as_deref(),
            Some("127.0.0.1:9876")
        );
        assert_eq!(
            NameServerAddressUtils::get_name_srv_addr_from_namesrv_endpoint(""),
            None
        );
    }

    #[test]
    fn name_server_endpoint_detection_matches_java_http_endpoint_prefix() {
        assert!(NameServerAddressUtils::is_name_srv_endpoint("http://127.0.0.1:9876"));
        assert!(!NameServerAddressUtils::is_name_srv_endpoint("127.0.0.1:9876"));
        assert!(!NameServerAddressUtils::is_name_srv_endpoint("https://127.0.0.1:9876"));
    }
}
