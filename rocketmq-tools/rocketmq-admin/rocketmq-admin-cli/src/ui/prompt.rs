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

use dialoguer::Confirm;
use dialoguer::Input;
use dialoguer::Select;
use dialoguer::theme::ColorfulTheme;

pub fn confirm(message: &str) -> bool {
    Confirm::with_theme(&ColorfulTheme::default())
        .with_prompt(message)
        .default(false)
        .interact()
        .unwrap_or(false)
}

pub fn confirm_default_yes(message: &str) -> bool {
    Confirm::with_theme(&ColorfulTheme::default())
        .with_prompt(message)
        .default(true)
        .interact()
        .unwrap_or(true)
}

pub fn input(message: &str) -> Option<String> {
    Input::with_theme(&ColorfulTheme::default())
        .with_prompt(message)
        .interact_text()
        .ok()
}

pub fn input_with_default(message: &str, default: &str) -> String {
    Input::with_theme(&ColorfulTheme::default())
        .with_prompt(message)
        .default(default.to_string())
        .interact_text()
        .unwrap_or_else(|_| default.to_string())
}

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

pub fn dangerous_operation_message(operation: &str, target: &str) -> String {
    format!("[WARNING] This will {operation} {target}. This action cannot be undone. Continue?")
}

pub fn confirm_dangerous_operation(operation: &str, target: &str) -> bool {
    confirm(&dangerous_operation_message(operation, target))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_dangerous_operation_messages() {
        let message = dangerous_operation_message("delete", "topic 'Test'");
        assert_eq!(
            message,
            "[WARNING] This will delete topic 'Test'. This action cannot be undone. Continue?"
        );
    }

    #[test]
    fn empty_selection_returns_none() {
        assert_eq!(select("Choose", &[]), None);
    }
}
