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

/// Default bounded first-pass correlation window.
pub const DEFAULT_CORRELATION_WINDOW_SECONDS: u32 = 300;

/// Floors a non-negative Unix timestamp into a deterministic bounded window.
///
/// `None` is returned for pre-epoch timestamps or a zero-width window.
#[must_use]
pub fn bounded_window_start_epoch(timestamp_seconds: i64, window_seconds: u32) -> Option<i64> {
    if timestamp_seconds < 0 || window_seconds == 0 {
        return None;
    }
    let width = i64::from(window_seconds);
    Some(timestamp_seconds - timestamp_seconds.rem_euclid(width))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floors_boundaries_without_clock_or_timezone_input() {
        assert_eq!(bounded_window_start_epoch(601, 300), Some(600));
        assert_eq!(bounded_window_start_epoch(899, 300), Some(600));
        assert_eq!(bounded_window_start_epoch(900, 300), Some(900));
        assert_eq!(bounded_window_start_epoch(-1, 300), None);
        assert_eq!(bounded_window_start_epoch(1, 0), None);
    }
}
