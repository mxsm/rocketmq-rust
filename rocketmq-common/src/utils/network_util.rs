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

use std::net::IpAddr;

pub struct NetworkUtil;

impl NetworkUtil {
    pub fn get_local_address() -> Option<String> {
        match local_ip_address::local_ip() {
            Ok(value) => match value {
                IpAddr::V4(ip) => Some(ip.to_string()),
                IpAddr::V6(ip) => Some(ip.to_string()),
            },
            Err(_) => match local_ip_address::local_ipv6() {
                Ok(value) => match value {
                    IpAddr::V4(ip) => Some(ip.to_string()),
                    IpAddr::V6(ip) => Some(ip.to_string()),
                },
                Err(_) => None,
            },
        }
    }
}
