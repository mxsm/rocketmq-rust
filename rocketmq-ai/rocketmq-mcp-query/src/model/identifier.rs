// Copyright 2026 The RocketMQ Rust Authors
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

pub(crate) const LOGICAL_ALIAS_MAX_BYTES: usize = 100;
pub(crate) const TOPIC_MAX_BYTES: usize = 127;
pub(crate) const CONSUMER_GROUP_MAX_BYTES: usize = 255;
pub(crate) const MESSAGE_ID_MAX_BYTES: usize = 256;

pub(crate) fn is_logical_alias(value: &str) -> bool {
    value == value.trim()
        && !value.is_empty()
        && value.len() <= LOGICAL_ALIAS_MAX_BYTES
        && !matches!(value, "." | "..")
        && value.parse::<std::net::IpAddr>().is_err()
        && value.parse::<std::net::SocketAddr>().is_err()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

pub(crate) fn is_topic(value: &str) -> bool {
    is_rocketmq_name(value, TOPIC_MAX_BYTES)
}

pub(crate) fn is_consumer_group(value: &str) -> bool {
    is_rocketmq_name(value, CONSUMER_GROUP_MAX_BYTES)
}

pub(crate) fn is_message_id(value: &str) -> bool {
    value == value.trim()
        && !value.is_empty()
        && value.len() <= MESSAGE_ID_MAX_BYTES
        && value.parse::<std::net::IpAddr>().is_err()
        && value.parse::<std::net::SocketAddr>().is_err()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

pub(crate) fn is_resource_filter(value: &str) -> bool {
    value == value.trim()
        && !value.is_empty()
        && value.len() <= TOPIC_MAX_BYTES
        && value.is_ascii()
        && !value.chars().any(char::is_control)
        && !value.contains('=')
        && !contains_encoded_resource_delimiter(value)
        && value.parse::<std::net::IpAddr>().is_err()
        && value.parse::<std::net::SocketAddr>().is_err()
}

pub(crate) fn is_sensitive_resource_value(value: &str) -> bool {
    if is_sensitive_plain_value(value) {
        return true;
    }
    let Ok(decoded) = percent_encoding::percent_decode_str(value).decode_utf8() else {
        return false;
    };
    decoded != value && is_sensitive_plain_value(&decoded)
}

pub(crate) fn contains_encoded_prompt_delimiter(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    ["%0a", "%0d", "%2f", "%3c", "%3d", "%3e", "%60", "%7b", "%7d"]
        .iter()
        .any(|delimiter| lower.contains(delimiter))
}

fn is_rocketmq_name(value: &str, max_bytes: usize) -> bool {
    value == value.trim()
        && !value.is_empty()
        && value.len() <= max_bytes
        && !contains_encoded_resource_delimiter(value)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'%' | b'|' | b'-' | b'_'))
}

fn contains_encoded_resource_delimiter(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    [
        "%20", "%22", "%23", "%25", "%26", "%2e", "%2f", "%3a", "%3c", "%3d", "%3e", "%3f", "%40", "%5b", "%5c", "%5d",
        "%60", "%7b", "%7d",
    ]
    .iter()
    .any(|delimiter| lower.contains(delimiter))
}

fn is_sensitive_plain_value(value: &str) -> bool {
    if value.contains('=') || value.parse::<std::net::IpAddr>().is_ok() || value.parse::<std::net::SocketAddr>().is_ok()
    {
        return true;
    }

    let lower = value.to_ascii_lowercase();
    [
        "access_key",
        "access-key",
        "secret_key",
        "secret-key",
        "client_secret",
        "client-secret",
        "token",
        "password",
        "private_key",
        "private-key",
        "message_body",
        "message-body",
    ]
    .iter()
    .any(|key| {
        lower.match_indices(key).any(|(offset, key)| {
            lower[offset + key.len()..]
                .chars()
                .next()
                .is_some_and(|separator| matches!(separator, ':' | ' ' | '\t' | '\'' | '"'))
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rocketmq_names_follow_the_project_ascii_contract() {
        for value in [
            "orders",
            "orders-v2",
            "%RETRY%orders",
            "%RETRY%abc",
            "%DLQ%deadbeef",
            "group|blue",
        ] {
            assert!(is_topic(value), "value={value}");
            assert!(is_consumer_group(value), "value={value}");
        }
        for value in [
            "127.0.0.1",
            "127.0.0.1:9876",
            "token=secret",
            "token%3Dsecret",
            "%74oken%3Dsecret",
            "127%2E0%2E0%2E1",
            "orders/topic",
            "orders.topic",
            "命令",
            "<b>orders</b>",
        ] {
            assert!(!is_topic(value), "value={value}");
            assert!(!is_consumer_group(value), "value={value}");
        }
        assert!(!is_topic(&"t".repeat(TOPIC_MAX_BYTES + 1)));
        assert!(!is_consumer_group(&"g".repeat(CONSUMER_GROUP_MAX_BYTES + 1)));
    }

    #[test]
    fn logical_aliases_and_message_ids_are_closed_safe_tokens() {
        assert!(is_logical_alias("local-dev"));
        assert!(is_message_id("7F000001-ABCD:42"));
        for value in [".", "..", "127.0.0.1", "token=secret", "broker/a"] {
            assert!(!is_logical_alias(value));
        }
        for value in ["run reset", "**reset**", "<script>", "命令", "token=secret"] {
            assert!(!is_message_id(value));
        }
        assert!(is_resource_filter("order/priority"));
        for value in ["127.0.0.1", "127.0.0.1:9876", "token=secret", "token%3Dsecret"] {
            assert!(!is_resource_filter(value));
        }
    }

    #[test]
    fn sensitive_resource_values_are_distinct_from_unrepresentable_names() {
        for value in [
            "127.0.0.1",
            "127.0.0.1:9876",
            "token=secret",
            "token:secret",
            "%74oken%3Dsecret",
            "127%2E0%2E0%2E1",
        ] {
            assert!(is_sensitive_resource_value(value), "value={value}");
        }
        for value in [".", ":", "orders.topic", &"x".repeat(TOPIC_MAX_BYTES + 1)] {
            assert!(!is_sensitive_resource_value(value), "value={value}");
        }
    }
}
