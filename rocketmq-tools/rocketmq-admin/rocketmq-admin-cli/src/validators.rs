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

use rocketmq_admin_core::core::RocketMQError;
use rocketmq_admin_core::core::RocketMQResult;
use rocketmq_admin_core::core::ToolsError;

pub fn validate_namesrv_addr(addr: &str) -> RocketMQResult<()> {
    if addr.is_empty() {
        return Err(RocketMQError::Tools(ToolsError::ValidationError {
            field: "namesrv_addr".to_string(),
            reason: "NameServer address cannot be empty".to_string(),
        }));
    }

    for single_addr in addr.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        let parts: Vec<&str> = single_addr.split(':').collect();

        if parts.len() != 2 {
            return Err(RocketMQError::Tools(ToolsError::ValidationError {
                field: "namesrv_addr".to_string(),
                reason: format!("Invalid format '{single_addr}', expected 'host:port'"),
            }));
        }

        parts[1].parse::<u16>().map_err(|_| {
            RocketMQError::Tools(ToolsError::ValidationError {
                field: "namesrv_addr".to_string(),
                reason: format!("Invalid port '{}' in address '{single_addr}'", parts[1]),
            })
        })?;
    }

    Ok(())
}

pub fn validate_topic_name(topic: &str) -> RocketMQResult<()> {
    if topic.is_empty() {
        return Err(RocketMQError::Tools(ToolsError::ValidationError {
            field: "topic".to_string(),
            reason: "Topic name cannot be empty".to_string(),
        }));
    }

    if topic.len() > 127 {
        return Err(RocketMQError::Tools(ToolsError::ValidationError {
            field: "topic".to_string(),
            reason: format!("Name '{topic}' exceeds maximum length of 127 characters"),
        }));
    }

    const INVALID_CHARS: &[char] = &['/', '\\', '|', '<', '>', '?', '*', '"', ':'];

    if let Some(ch) = topic.chars().find(|c| INVALID_CHARS.contains(c)) {
        return Err(RocketMQError::Tools(ToolsError::ValidationError {
            field: "topic".to_string(),
            reason: format!("Name '{topic}' contains invalid character '{ch}'"),
        }));
    }

    Ok(())
}

pub fn validate_queue_nums(nums: i32, name: &str) -> RocketMQResult<()> {
    match nums {
        n if n <= 0 => Err(RocketMQError::Tools(ToolsError::ValidationError {
            field: name.to_string(),
            reason: format!("must be positive, got {nums}"),
        })),
        n if n > 1024 => Err(RocketMQError::Tools(ToolsError::ValidationError {
            field: name.to_string(),
            reason: format!("exceeds maximum value of 1024, got {nums}"),
        })),
        _ => Ok(()),
    }
}

pub fn validate_perm(perm: i32) -> RocketMQResult<()> {
    match perm {
        2 | 4 | 6 => Ok(()),
        _ => Err(RocketMQError::Tools(ToolsError::ValidationError {
            field: "perm".to_string(),
            reason: format!("Invalid value {perm}, valid values are: 2 (read), 4 (write), 6 (read+write)"),
        })),
    }
}

pub fn validate_broker_name(name: &str) -> RocketMQResult<()> {
    match name {
        "" => Err(RocketMQError::Tools(ToolsError::ValidationError {
            field: "broker_name".to_string(),
            reason: "Broker name cannot be empty".to_string(),
        })),
        n if n.len() > 127 => Err(RocketMQError::Tools(ToolsError::ValidationError {
            field: "broker_name".to_string(),
            reason: format!("Name '{name}' exceeds maximum length of 127 characters"),
        })),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_namesrv_addresses() {
        assert!(validate_namesrv_addr("192.168.0.1:9876").is_ok());
        assert!(validate_namesrv_addr("192.168.0.1:9876;192.168.0.2:9876").is_ok());
        assert!(validate_namesrv_addr("").is_err());
        assert!(validate_namesrv_addr("192.168.0.1").is_err());
        assert!(validate_namesrv_addr("192.168.0.1:abc").is_err());
    }

    #[test]
    fn validates_topic_names() {
        assert!(validate_topic_name("test_topic").is_ok());
        assert!(validate_topic_name("").is_err());
        assert!(validate_topic_name(&"a".repeat(128)).is_err());
        assert!(validate_topic_name("topic/name").is_err());
    }

    #[test]
    fn validates_queue_nums() {
        assert!(validate_queue_nums(8, "read_queue_nums").is_ok());
        assert!(validate_queue_nums(-1, "read_queue_nums").is_err());
        assert!(validate_queue_nums(0, "read_queue_nums").is_err());
        assert!(validate_queue_nums(2000, "read_queue_nums").is_err());
    }

    #[test]
    fn validates_permissions() {
        assert!(validate_perm(2).is_ok());
        assert!(validate_perm(4).is_ok());
        assert!(validate_perm(6).is_ok());
        assert!(validate_perm(0).is_err());
        assert!(validate_perm(8).is_err());
    }

    #[test]
    fn validates_broker_names() {
        assert!(validate_broker_name("broker-a").is_ok());
        assert!(validate_broker_name("").is_err());
        assert!(validate_broker_name(&"a".repeat(128)).is_err());
    }
}
