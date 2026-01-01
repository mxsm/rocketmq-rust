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

use std::path::PathBuf;

// Default broker config path
pub fn get_broker_config_path() -> String {
    let mut path = dirs::home_dir().unwrap();
    path.push("store");
    path.push("config");
    path.push("broker.properties");
    path.to_string_lossy().into_owned()
}

// Topic config path
pub fn get_topic_config_path(root_dir: &str) -> String {
    PathBuf::from(root_dir)
        .join("config")
        .join("topics.json")
        .to_string_lossy()
        .into_owned()
}

// Topic-queue mapping path
pub fn get_topic_queue_mapping_path(root_dir: &str) -> String {
    PathBuf::from(root_dir)
        .join("config")
        .join("topicQueueMapping.json")
        .to_string_lossy()
        .into_owned()
}

// Consumer offset path
pub fn get_consumer_offset_path(root_dir: &str) -> String {
    PathBuf::from(root_dir)
        .join("config")
        .join("consumerOffset.json")
        .to_string_lossy()
        .into_owned()
}

// Lmq consumer offset path
pub fn get_lmq_consumer_offset_path(root_dir: &str) -> String {
    PathBuf::from(root_dir)
        .join("config")
        .join("lmqConsumerOffset.json")
        .to_string_lossy()
        .into_owned()
}

// Consumer order info path
pub fn get_consumer_order_info_path(root_dir: &str) -> String {
    PathBuf::from(root_dir)
        .join("config")
        .join("consumerOrderInfo.json")
        .to_string_lossy()
        .into_owned()
}

// Subscription group path
pub fn get_subscription_group_path(root_dir: &str) -> String {
    PathBuf::from(root_dir)
        .join("config")
        .join("subscriptionGroup.json")
        .to_string_lossy()
        .into_owned()
}

// Timer check path
pub fn get_timer_check_path(root_dir: &str) -> String {
    PathBuf::from(root_dir)
        .join("config")
        .join("timercheck")
        .to_string_lossy()
        .into_owned()
}

// Timer metrics path
pub fn get_timer_metrics_path(root_dir: &str) -> String {
    PathBuf::from(root_dir)
        .join("config")
        .join("timermetrics")
        .to_string_lossy()
        .into_owned()
}

// Transaction metrics path
pub fn get_transaction_metrics_path(root_dir: &str) -> String {
    PathBuf::from(root_dir)
        .join("config")
        .join("transactionMetrics")
        .to_string_lossy()
        .into_owned()
}

// Consumer filter path
pub fn get_consumer_filter_path(root_dir: &str) -> String {
    PathBuf::from(root_dir)
        .join("config")
        .join("consumerFilter.json")
        .to_string_lossy()
        .into_owned()
}

/// Returns the path to the message request mode configuration file.
///
/// This function constructs the full path to the configuration file by joining
/// the given root directory with the specific configuration subdirectory and filename.
///
/// # Arguments
///
/// * `root_dir` - A string slice representing the root directory.
///
/// # Returns
///
/// A `String` containing the full path to the "messageRequestMode.json" file.
pub fn get_message_request_mode_path(root_dir: &str) -> String {
    PathBuf::from(root_dir)
        .join("config")
        .join("messageRequestMode.json")
        .to_string_lossy()
        .into_owned()
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn test_get_broker_config_path() {
        let path = get_broker_config_path();
        let home_dir = dirs::home_dir().unwrap();
        let mut path_ = home_dir;
        path_.push("store");
        path_.push("config");
        path_.push("broker.properties");
        assert_eq!(path, path_.to_string_lossy().into_owned());
    }

    #[test]
    fn test_get_topic_config_path() {
        let root_dir = PathBuf::from("/path/to/root").to_string_lossy().into_owned();
        let path = get_topic_config_path(root_dir.as_str());
        let expected_path = PathBuf::from(root_dir.clone()).join("config").join("topics.json");
        assert_eq!(path, expected_path.to_string_lossy().into_owned());
    }

    #[test]
    fn test_get_topic_queue_mapping_path() {
        let root_dir = PathBuf::from("/path/to/root").to_string_lossy().into_owned();
        let path = get_topic_queue_mapping_path(root_dir.as_str());
        let expected_path = PathBuf::from(root_dir.clone())
            .join("config")
            .join("topicQueueMapping.json");
        assert_eq!(path, expected_path.to_string_lossy().into_owned());
    }

    #[test]
    fn test_get_consumer_offset_path() {
        let root_dir = PathBuf::from("/path/to/root").to_string_lossy().into_owned();
        let path = get_consumer_offset_path(root_dir.as_str());
        let expected_path = PathBuf::from(root_dir.clone())
            .join("config")
            .join("consumerOffset.json");
        assert_eq!(path, expected_path.to_string_lossy().into_owned());
    }

    #[test]
    fn test_get_lmq_consumer_offset_path() {
        let root_dir = PathBuf::from("/path/to/root").to_string_lossy().into_owned();
        let path = get_lmq_consumer_offset_path(root_dir.as_str());
        let expected_path = PathBuf::from(root_dir.clone())
            .join("config")
            .join("lmqConsumerOffset.json");
        assert_eq!(path, expected_path.to_string_lossy().into_owned());
    }

    #[test]
    fn test_get_consumer_order_info_path() {
        let root_dir = PathBuf::from("/path/to/root").to_string_lossy().into_owned();
        let path = get_consumer_order_info_path(root_dir.as_str());
        let expected_path = PathBuf::from(root_dir.clone())
            .join("config")
            .join("consumerOrderInfo.json");
        assert_eq!(path, expected_path.to_string_lossy().into_owned());
    }

    #[test]
    fn test_get_subscription_group_path() {
        let root_dir = PathBuf::from("/path/to/root").to_string_lossy().into_owned();
        let path = get_subscription_group_path(root_dir.as_str());
        let expected_path = PathBuf::from(root_dir.clone())
            .join("config")
            .join("subscriptionGroup.json");
        assert_eq!(path, expected_path.to_string_lossy().into_owned());
    }

    #[test]
    fn test_get_timer_check_path() {
        let root_dir = PathBuf::from("/path/to/root").to_string_lossy().into_owned();
        let path = get_timer_check_path(root_dir.as_str());
        let expected_path = PathBuf::from(root_dir.clone()).join("config").join("timercheck");
        assert_eq!(path, expected_path.to_string_lossy().into_owned());
    }

    #[test]
    fn test_get_timer_metrics_path() {
        let root_dir = PathBuf::from("/path/to/root").to_string_lossy().into_owned();
        let path = get_timer_metrics_path(root_dir.as_str());
        let expected_path = PathBuf::from(root_dir.clone()).join("config").join("timermetrics");
        assert_eq!(path, expected_path.to_string_lossy().into_owned());
    }

    #[test]
    fn test_get_transaction_metrics_path() {
        let root_dir = PathBuf::from("/path/to/root").to_string_lossy().into_owned();
        let path = get_transaction_metrics_path(root_dir.as_str());
        let expected_path = PathBuf::from(root_dir.clone())
            .join("config")
            .join("transactionMetrics");
        assert_eq!(path, expected_path.to_string_lossy().into_owned());
    }

    #[test]
    fn test_get_consumer_filter_path() {
        let root_dir = PathBuf::from("/path/to/root").to_string_lossy().into_owned();
        let path = get_consumer_filter_path(root_dir.as_str());
        let expected_path = PathBuf::from(root_dir.clone())
            .join("config")
            .join("consumerFilter.json");
        assert_eq!(path, expected_path.to_string_lossy().into_owned());
    }

    #[test]
    fn test_get_message_request_mode_path() {
        let root_dir = PathBuf::from("/path/to/root").to_string_lossy().into_owned();
        let path = get_message_request_mode_path(root_dir.as_str());
        let expected_path = PathBuf::from(root_dir.clone())
            .join("config")
            .join("messageRequestMode.json");
        assert_eq!(path, expected_path.to_string_lossy().into_owned());
    }
}
