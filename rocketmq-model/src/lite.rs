//  Copyright 2023 The RocketMQ Rust Authors
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

pub const SEPARATOR: char = '$';
pub const LITE_TOPIC_PREFIX: &str = "%LMQ%$";

pub fn to_lmq_name(parent_topic: &str, lite_topic: &str) -> Option<String> {
    if parent_topic.is_empty() || lite_topic.is_empty() {
        return None;
    }
    Some(format!("{}{}${}", LITE_TOPIC_PREFIX, parent_topic, lite_topic))
}

pub fn is_lite_topic_queue(lmq_name: &str) -> bool {
    !lmq_name.is_empty() && lmq_name.starts_with(LITE_TOPIC_PREFIX)
}

pub fn get_parent_topic(lmq_name: &str) -> Option<String> {
    if !is_lite_topic_queue(lmq_name) {
        return None;
    }

    let remaining = &lmq_name[LITE_TOPIC_PREFIX.len()..];
    let index = remaining.find(SEPARATOR)?;

    if index == 0 || index == remaining.len() - 1 {
        return None;
    }

    // Check that there's only one separator after the prefix
    if remaining[index + 1..].contains(SEPARATOR) {
        return None;
    }

    Some(remaining[..index].to_string())
}

pub fn get_lite_topic(lmq_name: &str) -> Option<String> {
    if !is_lite_topic_queue(lmq_name) {
        return None;
    }

    let remaining = &lmq_name[LITE_TOPIC_PREFIX.len()..];
    let index = remaining.find(SEPARATOR)?;

    if index == 0 || index == remaining.len() - 1 {
        return None;
    }

    // Check that there's only one separator after the prefix
    if remaining[index + 1..].contains(SEPARATOR) {
        return None;
    }

    Some(remaining[index + 1..].to_string())
}

pub fn get_parent_and_lite_topic(lmq_name: &str) -> Option<(String, String)> {
    if !lmq_name.starts_with(LITE_TOPIC_PREFIX) {
        return None;
    }

    let remaining = &lmq_name[LITE_TOPIC_PREFIX.len()..];
    let parts: Vec<&str> = remaining.split(SEPARATOR).collect();

    if parts.len() != 2 {
        return None;
    }

    if parts[0].is_empty() || parts[1].is_empty() {
        return None;
    }

    Some((parts[0].to_string(), parts[1].to_string()))
}

pub fn belongs_to(lmq_name: &str, parent_topic: &str) -> bool {
    let prefix = format!("{}{}{}", LITE_TOPIC_PREFIX, parent_topic, SEPARATOR);
    lmq_name.starts_with(&prefix)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_lmq_name() {
        let result = to_lmq_name("parent", "child");
        assert!(result.is_some());
        let lmq_name = result.unwrap();
        assert!(lmq_name.contains("parent"));
        assert!(lmq_name.contains("child"));
        assert!(lmq_name.contains("$"));
    }

    #[test]
    fn test_to_lmq_name_empty_parent() {
        let result = to_lmq_name("", "child");
        assert!(result.is_none());
    }

    #[test]
    fn test_to_lmq_name_empty_lite_topic() {
        let result = to_lmq_name("parent", "");
        assert!(result.is_none());
    }

    #[test]
    fn test_is_lite_topic_queue_valid() {
        let lmq_name = to_lmq_name("parent", "child").unwrap();
        assert!(is_lite_topic_queue(&lmq_name));
    }

    #[test]
    fn test_is_lite_topic_queue_invalid() {
        assert!(!is_lite_topic_queue("regular_topic"));
        assert!(!is_lite_topic_queue(""));
    }

    #[test]
    fn test_get_parent_topic() {
        let lmq_name = to_lmq_name("my_parent", "my_child").unwrap();
        let parent = get_parent_topic(&lmq_name);
        assert_eq!(parent, Some("my_parent".to_string()));
    }

    #[test]
    fn test_get_parent_topic_not_lite() {
        let parent = get_parent_topic("regular_topic");
        assert!(parent.is_none());
    }

    #[test]
    fn test_get_lite_topic() {
        let lmq_name = to_lmq_name("my_parent", "my_child").unwrap();
        let lite = get_lite_topic(&lmq_name);
        assert_eq!(lite, Some("my_child".to_string()));
    }

    #[test]
    fn test_get_lite_topic_not_lite() {
        let lite = get_lite_topic("regular_topic");
        assert!(lite.is_none());
    }

    #[test]
    fn test_get_parent_and_lite_topic() {
        let lmq_name = to_lmq_name("parent_test", "child_test").unwrap();
        let pair = get_parent_and_lite_topic(&lmq_name);
        assert!(pair.is_some());
        let (parent, child) = pair.unwrap();
        assert_eq!(parent, "parent_test");
        assert_eq!(child, "child_test");
    }

    #[test]
    fn test_get_parent_and_lite_topic_invalid() {
        let pair = get_parent_and_lite_topic("not_a_lite_topic");
        assert!(pair.is_none());
    }

    #[test]
    fn test_belongs_to_true() {
        let lmq_name = to_lmq_name("my_parent", "my_child").unwrap();
        assert!(belongs_to(&lmq_name, "my_parent"));
    }

    #[test]
    fn test_belongs_to_false() {
        let lmq_name = to_lmq_name("parent1", "child1").unwrap();
        assert!(!belongs_to(&lmq_name, "parent2"));
    }

    #[test]
    fn test_belongs_to_invalid() {
        assert!(!belongs_to("regular_topic", "parent"));
    }

    #[test]
    fn test_roundtrip() {
        let original_parent = "test_parent";
        let original_child = "test_child";

        let lmq_name = to_lmq_name(original_parent, original_child).unwrap();

        let extracted_parent = get_parent_topic(&lmq_name).unwrap();
        let extracted_child = get_lite_topic(&lmq_name).unwrap();

        assert_eq!(extracted_parent, original_parent);
        assert_eq!(extracted_child, original_child);
    }

    #[test]
    fn test_special_characters_in_names() {
        let parent = "parent-123";
        let child = "child_456";

        let lmq_name = to_lmq_name(parent, child).unwrap();

        assert_eq!(get_parent_topic(&lmq_name), Some(parent.to_string()));
        assert_eq!(get_lite_topic(&lmq_name), Some(child.to_string()));
    }

    #[test]
    fn test_multiple_separators_in_name() {
        // Create a malformed lmq_name with extra separators
        let malformed = format!("{}parent{}child{}", LITE_TOPIC_PREFIX, SEPARATOR, SEPARATOR);

        assert!(is_lite_topic_queue(&malformed));
        // But parsing should fail due to multiple separators
        assert!(get_parent_topic(&malformed).is_none());
        assert!(get_lite_topic(&malformed).is_none());
    }
}
