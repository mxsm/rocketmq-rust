/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::env;

use regex::Regex;

pub struct NameServerAddressUtils;

impl NameServerAddressUtils {
    pub const INSTANCE_PREFIX: &'static str = "MQ_INST_";
    pub const INSTANCE_REGEX: &'static str = r"MQ_INST_\w+_\w+";
    pub const ENDPOINT_PREFIX: &'static str = r"(\w+://|)";
    pub const NAMESRV_ENDPOINT_PATTERN: &'static str = r"^http://.*";
    pub const INST_ENDPOINT_PATTERN: &'static str = r"^(\w+://|)MQ_INST_\w+_\w+\..*";

    pub fn get_name_server_addresses() -> Option<String> {
        env::var("NAMESRV_ADDR_PROPERTY")
            .or_else(|_| env::var("NAMESRV_ADDR_ENV"))
            .ok()
    }

    pub fn validate_instance_endpoint(endpoint: &str) -> bool {
        let re = Regex::new(Self::INST_ENDPOINT_PATTERN).unwrap();
        re.is_match(endpoint)
    }

    pub fn parse_instance_id_from_endpoint(endpoint: &str) -> Option<String> {
        if endpoint.is_empty() {
            return None;
        }
        let start = endpoint.rfind('/')? + 1;
        let end = endpoint.find('.')?;
        Some(endpoint[start..end].to_string())
    }

    pub fn get_name_srv_addr_from_namesrv_endpoint(name_srv_endpoint: &str) -> Option<String> {
        if name_srv_endpoint.is_empty() {
            return None;
        }
        let start = name_srv_endpoint.rfind('/')? + 1;
        Some(name_srv_endpoint[start..].to_string())
    }
}
