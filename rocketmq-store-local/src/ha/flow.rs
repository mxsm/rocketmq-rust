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

use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;

/// Canonical byte-budget state for one HA transfer connection.
pub struct FlowControlWindow {
    enabled: bool,
    max_bytes_per_second: usize,
    transferred_bytes: AtomicI64,
    transferred_bytes_per_second: AtomicI64,
}

impl FlowControlWindow {
    pub const fn new(enabled: bool, max_bytes_per_second: usize) -> Self {
        Self {
            enabled,
            max_bytes_per_second,
            transferred_bytes: AtomicI64::new(0),
            transferred_bytes_per_second: AtomicI64::new(0),
        }
    }

    pub fn roll_window(&self) {
        let current = self.transferred_bytes.swap(0, Ordering::Relaxed);
        self.transferred_bytes_per_second.store(current, Ordering::Relaxed);
    }

    pub fn record_transferred(&self, count: i64) {
        self.transferred_bytes.fetch_add(count, Ordering::Relaxed);
    }

    pub fn transferred_bytes_per_second(&self) -> i64 {
        self.transferred_bytes_per_second.load(Ordering::Relaxed)
    }

    pub fn available_bytes(&self) -> i32 {
        if !self.enabled {
            return i32::MAX;
        }

        let max_bytes = i64::try_from(self.max_bytes_per_second).unwrap_or(i64::MAX);
        let available = max_bytes
            .saturating_sub(self.transferred_bytes.load(Ordering::Relaxed))
            .max(0);
        i32::try_from(available).unwrap_or(i32::MAX)
    }

    pub const fn max_bytes_per_second(&self) -> usize {
        self.max_bytes_per_second
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enabled_window_caps_available_bytes_and_rolls_speed() {
        let window = FlowControlWindow::new(true, 200);
        window.record_transferred(150);
        assert_eq!(window.available_bytes(), 50);
        window.roll_window();
        assert_eq!(window.transferred_bytes_per_second(), 150);
        assert_eq!(window.available_bytes(), 200);
    }

    #[test]
    fn disabled_window_is_unbounded() {
        let window = FlowControlWindow::new(false, 1);
        window.record_transferred(i64::MAX);
        assert_eq!(window.available_bytes(), i32::MAX);
    }
}
