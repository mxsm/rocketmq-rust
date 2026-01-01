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

//! Table style utilities for consistent formatting
//!
//! Provides constants and helper functions for table styling.

/// Cyan color constant for headers
pub const HEADER_COLOR: &str = "\x1b[36m";
/// Reset color constant
pub const RESET_COLOR: &str = "\x1b[0m";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_colors() {
        assert_eq!(HEADER_COLOR, "\x1b[36m");
        assert_eq!(RESET_COLOR, "\x1b[0m");
    }
}
