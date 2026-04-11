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

use indicatif::ProgressBar;
use indicatif::ProgressStyle;

pub fn create_spinner(message: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .tick_strings(&["-", "\\", "|", "/"])
            .template("{spinner:.blue} {msg}")
            .expect("valid spinner template"),
    );
    pb.set_message(message.to_string());
    pb.enable_steady_tick(std::time::Duration::from_millis(80));
    pb
}

pub fn create_progress_bar(total: u64, message: &str) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{msg} [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .expect("valid progress bar template")
            .progress_chars("=>-"),
    );
    pb.set_message(message.to_string());
    pb
}

pub fn finish_progress_success(pb: &ProgressBar, message: &str) {
    pb.finish_with_message(format!("[OK] {message}"));
}

pub fn finish_progress_error(pb: &ProgressBar, message: &str) {
    pb.finish_with_message(format!("[ERROR] {message}"));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_helpers_do_not_panic() {
        let spinner = create_spinner("Loading");
        spinner.finish_and_clear();

        let pb = create_progress_bar(10, "Counting");
        for _ in 0..10 {
            pb.inc(1);
        }
        finish_progress_success(&pb, "Done");

        let pb = create_progress_bar(1, "Failing");
        finish_progress_error(&pb, "Failed");
    }
}
