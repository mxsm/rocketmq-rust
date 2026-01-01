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

use regex::Regex;

use crate::common::mix_all;

const INSTANCE_PREFIX: &str = "MQ_INST_";
const INSTANCE_REGEX: &str = "MQ_INST_\\w+_\\w+";
const ENDPOINT_PREFIX: &str = "(\\w+://|)";

pub static NAMESRV_ENDPOINT_PATTERN: std::sync::LazyLock<Regex> =
    std::sync::LazyLock::new(|| Regex::new("^http://.*").unwrap());

pub static INST_ENDPOINT_PATTERN: std::sync::LazyLock<Regex> =
    std::sync::LazyLock::new(|| Regex::new(&format!("^{ENDPOINT_PREFIX}{INSTANCE_REGEX}")).unwrap());

pub struct NameServerAddressUtils;

impl NameServerAddressUtils {
    pub fn get_name_server_addresses() -> Option<String> {
        env::var(mix_all::NAMESRV_ADDR_PROPERTY)
            .or_else(|_| env::var(mix_all::NAMESRV_ADDR_ENV))
            .ok()
    }

    pub fn validate_instance_endpoint(endpoint: &str) -> bool {
        INST_ENDPOINT_PATTERN.is_match(endpoint)
    }

    pub fn parse_instance_id_from_endpoint(endpoint: &str) -> Option<String> {
        if endpoint.is_empty() {
            None
        } else {
            let last_slash = endpoint.rfind('/').unwrap_or(0);
            let dot_pos = endpoint.find('.').unwrap_or(endpoint.len());
            Some(endpoint[last_slash + 1..dot_pos].to_string())
        }
    }

    pub fn get_name_srv_addr_from_namesrv_endpoint(name_srv_endpoint: &str) -> Option<String> {
        if name_srv_endpoint.is_empty() {
            None
        } else {
            let last_slash = name_srv_endpoint.rfind('/').unwrap_or(0);
            Some(name_srv_endpoint[last_slash + 1..].to_string())
        }
    }
}
