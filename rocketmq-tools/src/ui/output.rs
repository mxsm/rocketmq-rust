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

//! Enhanced output formatting with colors and styles

use colored::Colorize;

/// Print success message with green checkmark
pub fn print_success(message: &str) {
    println!("{} {}", "[OK]".green().bold(), message);
}

/// Print error message with red cross
pub fn print_error(message: &str) {
    eprintln!("{} {}", "[ERROR]".red().bold(), message);
}

/// Print warning message with yellow exclamation
pub fn print_warning(message: &str) {
    println!("{} {}", "⚠".yellow().bold(), message);
}

/// Print info message with blue icon
pub fn print_info(message: &str) {
    println!("{} {}", "[INFO]".blue().bold(), message);
}

/// Print section header
pub fn print_header(title: &str) {
    println!("\n{}", title.cyan().bold().underline());
}

/// Print sub-header
pub fn print_subheader(title: &str) {
    println!("\n{}", title.cyan().bold());
}

/// Print key-value pair
pub fn print_key_value(key: &str, value: &str) {
    println!("  {}: {}", key.bright_white().bold(), value.bright_white());
}

/// Print operation result summary
pub fn print_summary(operation: &str, success: bool, details: Option<&str>) {
    if success {
        print_success(&format!("{} completed successfully", operation));
    } else {
        print_error(&format!("{} failed", operation));
    }

    if let Some(details) = details {
        println!("  {}", details.bright_black());
    }
}

/// Print a formatted list
pub fn print_list(title: &str, items: &[String]) {
    print_header(title);
    for (i, item) in items.iter().enumerate() {
        println!("  {}. {}", (i + 1).to_string().bright_black(), item);
    }
}

/// Format count with proper pluralization
pub fn format_count(count: usize, singular: &str, plural: &str) -> String {
    if count == 1 {
        format!("{} {}", count, singular)
    } else {
        format!("{} {}", count, plural)
    }
}

/// Print empty result message
pub fn print_empty_result(entity: &str) {
    print_info(&format!("No {} found", entity));
}

/// Print operation starting
pub fn print_operation_start(operation: &str) {
    println!("{} {}...", "▶".blue().bold(), operation);
}

/// Print divider line
pub fn print_divider() {
    println!("{}", "─".repeat(80).bright_black());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_count() {
        assert_eq!(format_count(1, "topic", "topics"), "1 topic");
        assert_eq!(format_count(5, "topic", "topics"), "5 topics");
        assert_eq!(format_count(0, "broker", "brokers"), "0 brokers");
    }

    #[test]
    fn test_print_functions() {
        // Test that print functions don't panic
        print_success("Test success");
        print_error("Test error");
        print_warning("Test warning");
        print_info("Test info");
    }

    #[test]
    fn test_print_header() {
        // Test header formatting doesn't panic
        print_header("Test Header");
        print_subheader("Test Subheader");
    }

    #[test]
    fn test_print_key_value() {
        // Test key-value formatting
        print_key_value("Name", "TestTopic");
        print_key_value("Status", "Active");
    }

    #[test]
    fn test_print_list() {
        let items = vec!["Item1".to_string(), "Item2".to_string(), "Item3".to_string()];
        print_list("Test List", &items);
    }

    #[test]
    fn test_print_empty_result() {
        print_empty_result("topics");
        print_empty_result("brokers");
    }

    #[test]
    fn test_print_operation_start() {
        print_operation_start("Testing operation");
    }

    #[test]
    fn test_print_divider() {
        print_divider();
    }

    #[test]
    fn test_format_count_edge_cases() {
        assert_eq!(format_count(1, "item", "items"), "1 item");
        assert_eq!(format_count(2, "item", "items"), "2 items");
        assert_eq!(format_count(0, "item", "items"), "0 items");
        assert_eq!(format_count(100, "item", "items"), "100 items");
    }
}
