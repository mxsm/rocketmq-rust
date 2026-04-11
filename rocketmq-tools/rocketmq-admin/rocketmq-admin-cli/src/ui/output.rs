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

use colored::Colorize;

pub fn print_success(message: &str) {
    println!("{} {}", "[OK]".green().bold(), message);
}

pub fn print_error(message: &str) {
    eprintln!("{} {}", "[ERROR]".red().bold(), message);
}

pub fn print_warning(message: &str) {
    println!("{} {}", "[WARN]".yellow().bold(), message);
}

pub fn print_info(message: &str) {
    println!("{} {}", "[INFO]".blue().bold(), message);
}

pub fn print_header(title: &str) {
    println!("\n{}", title.cyan().bold().underline());
}

pub fn print_subheader(title: &str) {
    println!("\n{}", title.cyan().bold());
}

pub fn print_key_value(key: &str, value: &str) {
    println!("  {}: {}", key.bright_white().bold(), value.bright_white());
}

pub fn print_summary(operation: &str, success: bool, details: Option<&str>) {
    if success {
        print_success(&format!("{operation} completed successfully"));
    } else {
        print_error(&format!("{operation} failed"));
    }

    if let Some(details) = details {
        println!("  {}", details.bright_black());
    }
}

pub fn print_list(title: &str, items: &[String]) {
    print_header(title);
    for (i, item) in items.iter().enumerate() {
        println!("  {}. {}", (i + 1).to_string().bright_black(), item);
    }
}

pub fn format_count(count: usize, singular: &str, plural: &str) -> String {
    if count == 1 {
        format!("{count} {singular}")
    } else {
        format!("{count} {plural}")
    }
}

pub fn print_empty_result(entity: &str) {
    print_info(&format!("No {entity} found"));
}

pub fn print_operation_start(operation: &str) {
    println!("{} {operation}...", "[RUN]".blue().bold());
}

pub fn print_divider() {
    println!("{}", "-".repeat(80).bright_black());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_counts() {
        assert_eq!(format_count(1, "topic", "topics"), "1 topic");
        assert_eq!(format_count(5, "topic", "topics"), "5 topics");
        assert_eq!(format_count(0, "broker", "brokers"), "0 brokers");
    }

    #[test]
    fn print_helpers_do_not_panic() {
        print_success("success");
        print_error("error");
        print_warning("warning");
        print_info("info");
        print_header("Header");
        print_subheader("Subheader");
        print_key_value("Name", "TestTopic");
        print_summary("Operation", true, Some("details"));
        print_list("Items", &["one".to_string(), "two".to_string()]);
        print_empty_result("topics");
        print_operation_start("Running");
        print_divider();
    }
}
