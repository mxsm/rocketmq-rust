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

use crate::errors::argument_invalid;
use rocketmq_error::Result as CanonicalResult;
use std::net::Ipv4Addr;

pub fn validate_namesrv_addr(addr: &str) -> CanonicalResult<()> {
    let mut has_valid_addr = false;
    for single_addr in addr.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        has_valid_addr = true;
        let (host, port_str) = single_addr
            .split_once(':')
            .ok_or_else(|| argument_invalid(format!("Invalid format '{single_addr}', expected 'host:port'")))?;
        if port_str.contains(':') {
            return Err(argument_invalid(format!(
                "Invalid format '{single_addr}', expected 'host:port'"
            )));
        }
        port_str
            .parse::<u16>()
            .map_err(|_| argument_invalid(format!("Invalid port '{port_str}' in address '{single_addr}'")))?;
        if host.parse::<Ipv4Addr>().is_err()
            && (host.is_empty()
                || host.contains(' ')
                || host.starts_with('.')
                || host.ends_with('.')
                || host.starts_with('-')
                || host.ends_with('-'))
        {
            return Err(argument_invalid(format!(
                "Invalid host '{host}' in address '{single_addr}'"
            )));
        }
    }
    if !has_valid_addr {
        return Err(argument_invalid("NameServer address cannot be empty"));
    }
    Ok(())
}

pub fn validate_topic_name(topic: &str) -> CanonicalResult<()> {
    if topic.is_empty() {
        return Err(argument_invalid("Topic name cannot be empty"));
    }

    if topic.len() > 127 {
        return Err(argument_invalid(format!(
            "Name '{topic}' exceeds maximum length of 127 bytes"
        )));
    }

    const INVALID_CHARS: &[char] = &['/', '\\', '|', '<', '>', '?', '*', '"', ':'];

    if let Some(ch) = topic.chars().find(|c| INVALID_CHARS.contains(c)) {
        return Err(argument_invalid(format!(
            "Name '{topic}' contains invalid character '{ch}'"
        )));
    }

    Ok(())
}

pub fn validate_queue_nums(nums: i32, name: &str) -> CanonicalResult<()> {
    match nums {
        n if n <= 0 => Err(argument_invalid(format!("{name} must be positive, got {nums}"))),
        n if n > 1024 => Err(argument_invalid(format!(
            "{name} exceeds maximum value of 1024, got {nums}"
        ))),
        _ => Ok(()),
    }
}

pub fn validate_perm(perm: i32) -> CanonicalResult<()> {
    match perm {
        2 | 4 | 6 => Ok(()),
        _ => Err(argument_invalid(format!(
            "Invalid value {perm}, valid values are: 2 (read), 4 (write), 6 (read+write)"
        ))),
    }
}

pub fn validate_broker_name(name: &str) -> CanonicalResult<()> {
    match name {
        "" => Err(argument_invalid("Broker name cannot be empty")),
        n if n.len() > 127 => Err(argument_invalid(format!(
            "Name '{name}' exceeds maximum length of 127 characters"
        ))),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_namesrv_addresses() {
        for valid_addr in [
            "192.168.0.1:9876",
            "192.168.0.1:9876;192.168.0.2:9876",
            "host:9876;",
            " host:9876 ",
            "host:65535",
        ] {
            assert!(validate_namesrv_addr(valid_addr).is_ok())
        }
        for invalid_addr in [
            "",
            "192.168.0.1",
            "192.168.0.1:abc",
            ";;;",
            "host:9876:53",
            ":123",
            "host:",
            "host:65536",
            "[::1]:9876",
        ] {
            let error = validate_namesrv_addr(invalid_addr).unwrap_err();
            assert_eq!(error.descriptor().code().as_str(), "core.argument.invalid");
        }
    }

    #[test]
    fn validates_topic_names() {
        for valid_str in [&"a".repeat(127), &"a".repeat(43), "test_topic"] {
            assert!(validate_topic_name(valid_str).is_ok());
        }
        for invalid_str in [
            "/",
            "\\",
            "|",
            "<",
            ">",
            "?",
            "*",
            "\"",
            ":",
            "",
            &"a".repeat(128),
            "topic/name",
            &"\u{4e3b}".repeat(43),
        ] {
            let error = validate_topic_name(invalid_str).unwrap_err();
            assert_eq!(error.descriptor().code().as_str(), "core.argument.invalid");
        }
    }

    #[test]
    fn validates_queue_nums() {
        for valid_num in [1, 8, 1024] {
            assert!(validate_queue_nums(valid_num, "read_queue_nums").is_ok())
        }
        for invalid_num in [i32::MIN, -1, 0, 1025, 2000, i32::MAX] {
            let error = validate_queue_nums(invalid_num, "read_queue_nums").unwrap_err();
            assert_eq!(error.descriptor().code().as_str(), "core.argument.invalid");
        }
    }

    #[test]
    fn validates_permissions() {
        for valid_perm in [2, 4, 6] {
            assert!(validate_perm(valid_perm).is_ok())
        }
        for invalid_perm in [-1, 0, 1, 3, 5, 7, 8] {
            let err = validate_perm(invalid_perm).unwrap_err();
            assert_eq!(err.descriptor().code().as_str(), "core.argument.invalid");
        }
    }

    #[test]
    fn validates_broker_names() {
        for valid_name in ["broker-a", " ", &"a".repeat(127)] {
            assert!(validate_broker_name(valid_name).is_ok());
        }
        for invalid_name in ["", &"a".repeat(128)] {
            let error = validate_broker_name(invalid_name).unwrap_err();
            assert_eq!(error.descriptor().code().as_str(), "core.argument.invalid");
        }
    }
}
