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

use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CleanupPolicy {
    #[default]
    DELETE,
    COMPACTION,
}

impl fmt::Display for CleanupPolicy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CleanupPolicy::DELETE => write!(f, "DELETE"),
            CleanupPolicy::COMPACTION => write!(f, "COMPACTION"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ParseCleanupPolicyError;

impl fmt::Display for ParseCleanupPolicyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "invalid cleanup policy")
    }
}

impl FromStr for CleanupPolicy {
    type Err = ParseCleanupPolicyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "DELETE" => Ok(CleanupPolicy::DELETE),
            "COMPACTION" => Ok(CleanupPolicy::COMPACTION),
            _ => Err(ParseCleanupPolicyError),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_policy_display() {
        assert_eq!(CleanupPolicy::DELETE.to_string(), "DELETE");
        assert_eq!(CleanupPolicy::COMPACTION.to_string(), "COMPACTION");
    }

    #[test]
    fn cleanup_policy_from_str() {
        assert_eq!("DELETE".parse(), Ok(CleanupPolicy::DELETE));
        assert_eq!("COMPACTION".parse(), Ok(CleanupPolicy::COMPACTION));
    }

    #[test]
    fn cleanup_policy_from_str_case_insensitive() {
        assert_eq!("delete".parse(), Ok(CleanupPolicy::DELETE));
        assert_eq!("compaction".parse(), Ok(CleanupPolicy::COMPACTION));
    }

    #[test]
    fn cleanup_policy_from_str_invalid() {
        assert!("invalid".parse::<CleanupPolicy>().is_err());
    }
}
