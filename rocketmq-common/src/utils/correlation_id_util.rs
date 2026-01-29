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

use uuid::Uuid;

pub struct CorrelationIdUtil;

impl CorrelationIdUtil {
    /// Creates a new correlation ID using UUID v4 in standard format.
    ///
    /// Uses hyphenated format (36 chars) to maintain compatibility with Java's UUID.toString().
    /// Format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    ///
    /// # Returns
    /// A 36-character UUID string (e.g., "550e8400-e29b-41d4-a716-446655440000")
    #[inline]
    pub fn create_correlation_id() -> String {
        Uuid::new_v4().to_string()
    }

    /// Creates a new correlation ID using UUID v4 in simple format (without hyphens).
    ///
    /// This format is more compact and has ~11% less memory overhead compared to standard format.
    /// Format: xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx (32 hex characters)
    ///
    /// # Returns
    /// A 32-character UUID string (e.g., "550e8400e29b41d4a716446655440000")
    #[inline]
    pub fn create_correlation_id_simple() -> String {
        Uuid::new_v4().simple().to_string()
    }
}
