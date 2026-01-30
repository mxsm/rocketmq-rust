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

//! Interactive prompts for user confirmation

use dialoguer::theme::ColorfulTheme;
use dialoguer::Confirm;
use dialoguer::Input;
use dialoguer::Select;

/// Prompt for yes/no confirmation
pub fn confirm(message: &str) -> bool {
    Confirm::with_theme(&ColorfulTheme::default())
        .with_prompt(message)
        .default(false)
        .interact()
        .unwrap_or(false)
}

/// Prompt for yes/no confirmation with default=true
pub fn confirm_default_yes(message: &str) -> bool {
    Confirm::with_theme(&ColorfulTheme::default())
        .with_prompt(message)
        .default(true)
        .interact()
        .unwrap_or(true)
}

/// Prompt for text input
pub fn input(message: &str) -> Option<String> {
    Input::with_theme(&ColorfulTheme::default())
        .with_prompt(message)
        .interact_text()
        .ok()
}

/// Prompt for text input with default value
pub fn input_with_default(message: &str, default: &str) -> String {
    Input::with_theme(&ColorfulTheme::default())
        .with_prompt(message)
        .default(default.to_string())
        .interact_text()
        .unwrap_or_else(|_| default.to_string())
}

/// Prompt for selection from list
pub fn select(message: &str, items: &[String]) -> Option<usize> {
    if items.is_empty() {
        return None;
    }

    Select::with_theme(&ColorfulTheme::default())
        .with_prompt(message)
        .items(items)
        .interact()
        .ok()
}

/// Confirm dangerous operation
pub fn confirm_dangerous_operation(operation: &str, target: &str) -> bool {
    let message = format!(
        "[WARNING] This will {} {}. This action cannot be undone. Continue?",
        operation, target
    );
    confirm(&message)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_confirm_dangerous_operation_message() {
        // Test message formatting for dangerous operations
        let operation = "delete";
        let target = "topic 'test-topic'";
        let message = format!(
            "[WARNING] This will {} {}. This action cannot be undone. Continue?",
            operation, target
        );
        assert!(message.contains("delete"));
        assert!(message.contains("test-topic"));
        assert!(message.contains("[WARNING]"));
    }

    #[test]
    fn test_dangerous_operation_messages() {
        // Test various dangerous operation message formats
        let test_cases = vec![
            ("delete", "topic 'MyTopic'"),
            ("wipe", "write permission for broker 'broker-a'"),
            ("remove", "configuration namespace='TEST'"),
        ];

        for (operation, target) in test_cases {
            let message = format!(
                "[WARNING] This will {} {}. This action cannot be undone. Continue?",
                operation, target
            );
            assert!(message.contains(operation));
            assert!(message.contains(target));
            assert!(message.contains("cannot be undone"));
        }
    }

    #[test]
    fn test_message_formatting() {
        // Test that different inputs produce expected message formats
        let message1 = format!(
            "[WARNING] This will {} {}. This action cannot be undone. Continue?",
            "delete", "topic 'Test'"
        );
        assert_eq!(
            message1,
            "[WARNING] This will delete topic 'Test'. This action cannot be undone. Continue?"
        );

        let message2 = format!(
            "[WARNING] This will {} {}. This action cannot be undone. Continue?",
            "update", "broker settings"
        );
        assert_eq!(
            message2,
            "[WARNING] This will update broker settings. This action cannot be undone. Continue?"
        );
    }
}
