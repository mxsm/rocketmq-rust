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

//! Progress indicators for long-running operations

use indicatif::ProgressBar;
use indicatif::ProgressStyle;

/// Create a spinner for indeterminate operations
pub fn create_spinner(message: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"])
            .template("{spinner:.blue} {msg}")
            .unwrap(),
    );
    pb.set_message(message.to_string());
    pb.enable_steady_tick(std::time::Duration::from_millis(80));
    pb
}

/// Create a progress bar for determinate operations
pub fn create_progress_bar(total: u64, message: &str) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{msg} [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap()
            .progress_chars("=>-"),
    );
    pb.set_message(message.to_string());
    pb
}

/// Finish progress with success message
pub fn finish_progress_success(pb: &ProgressBar, message: &str) {
    pb.finish_with_message(format!("[OK] {}", message));
}

/// Finish progress with error message
pub fn finish_progress_error(pb: &ProgressBar, message: &str) {
    pb.finish_with_message(format!("[ERROR] {}", message));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spinner_creation() {
        let spinner = create_spinner("Testing...");
        spinner.finish();
    }

    #[test]
    fn test_progress_bar_creation() {
        let pb = create_progress_bar(100, "Processing");
        pb.finish();
    }

    #[test]
    fn test_spinner_with_different_messages() {
        let spinner1 = create_spinner("Connecting...");
        let spinner2 = create_spinner("Fetching data...");
        let spinner3 = create_spinner("Processing...");

        spinner1.finish();
        spinner2.finish();
        spinner3.finish();
    }

    #[test]
    fn test_progress_bar_with_different_totals() {
        let pb1 = create_progress_bar(10, "Small task");
        let pb2 = create_progress_bar(1000, "Large task");
        let pb3 = create_progress_bar(0, "Empty task");

        pb1.finish();
        pb2.finish();
        pb3.finish();
    }

    #[test]
    fn test_finish_progress_success() {
        let pb = create_progress_bar(100, "Task");
        finish_progress_success(&pb, "Completed successfully");
    }

    #[test]
    fn test_finish_progress_error() {
        let pb = create_progress_bar(100, "Task");
        finish_progress_error(&pb, "Failed with error");
    }

    #[test]
    fn test_spinner_finish_and_clear() {
        let spinner = create_spinner("Loading...");
        spinner.finish_and_clear();
    }

    #[test]
    fn test_progress_bar_increment() {
        let pb = create_progress_bar(10, "Counting");
        for _ in 0..10 {
            pb.inc(1);
        }
        finish_progress_success(&pb, "Done");
    }
}
