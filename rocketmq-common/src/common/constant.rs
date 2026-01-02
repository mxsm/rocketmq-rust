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

pub mod consume_init_mode;

use std::ops::Deref;

pub struct PermName;

impl PermName {
    pub const INDEX_PERM_INHERIT: u32 = 0;
    pub const INDEX_PERM_PRIORITY: u32 = 3;
    pub const INDEX_PERM_READ: u32 = 2;
    pub const INDEX_PERM_WRITE: u32 = 1;
    pub const PERM_INHERIT: u32 = 0x1 << Self::INDEX_PERM_INHERIT;
    pub const PERM_PRIORITY: u32 = 0x1 << Self::INDEX_PERM_PRIORITY;
    pub const PERM_READ: u32 = 0x1 << Self::INDEX_PERM_READ;
    pub const PERM_WRITE: u32 = 0x1 << Self::INDEX_PERM_WRITE;

    #[inline]
    pub fn perm2string(perm: u32) -> String {
        let mut sb = String::from("---");

        if Self::is_readable(perm) {
            sb.replace_range(0..1, "R");
        }

        if Self::is_writeable(perm) {
            sb.replace_range(1..2, "W");
        }

        if Self::is_inherited(perm) {
            sb.replace_range(2..3, "X");
        }

        sb
    }

    #[inline]
    pub fn perm_to_string(perm: u32) -> String {
        let mut s = ['-', '-', '-'];
        if Self::is_readable(perm) {
            s[0] = 'R';
        }
        if Self::is_writeable(perm) {
            s[1] = 'W';
        }
        if Self::is_inherited(perm) {
            s[2] = 'X';
        }
        s.iter().collect()
    }

    #[inline]
    pub fn is_readable(perm: u32) -> bool {
        (perm & Self::PERM_READ) == Self::PERM_READ
    }

    #[inline]
    pub fn is_writeable(perm: u32) -> bool {
        (perm & Self::PERM_WRITE) == Self::PERM_WRITE
    }

    #[inline]
    pub fn is_inherited(perm: u32) -> bool {
        (perm & Self::PERM_INHERIT) == Self::PERM_INHERIT
    }

    #[inline]
    pub fn is_valid(perm: u32) -> bool {
        perm < Self::PERM_PRIORITY
    }

    #[inline]
    pub fn is_priority(perm: u32) -> bool {
        (perm & Self::PERM_PRIORITY) == Self::PERM_PRIORITY
    }

    #[inline]
    pub fn is_valid_str(perm: &str) -> bool {
        perm.parse::<u32>().ok().map(Self::is_valid).unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::PermName;

    #[test]
    fn test_perm_2_string() {
        assert_eq!(PermName::perm_to_string(0).as_str(), "---");
        assert_eq!(PermName::perm_to_string(PermName::PERM_READ).as_str(), "R--");
        assert_eq!(
            PermName::perm_to_string(PermName::PERM_READ | PermName::PERM_WRITE).as_str(),
            "RW-"
        );

        assert_eq!(
            PermName::perm_to_string(PermName::PERM_READ | PermName::PERM_WRITE | PermName::PERM_INHERIT),
            "RWX"
        );
    }

    #[test]
    fn test_is_readable() {
        assert!(!PermName::is_readable(0));
        assert!(PermName::is_readable(PermName::PERM_READ));
        assert!(PermName::is_readable(PermName::PERM_READ | PermName::PERM_WRITE));
        assert!(PermName::is_readable(
            PermName::PERM_READ | PermName::PERM_WRITE | PermName::PERM_PRIORITY
        ));
        assert!(PermName::is_readable(
            PermName::PERM_READ | PermName::PERM_WRITE | PermName::PERM_PRIORITY | PermName::PERM_INHERIT
        ));
    }

    #[test]
    fn test_is_writable() {
        assert!(!PermName::is_writeable(0));
        assert!(PermName::is_writeable(PermName::PERM_WRITE));
        assert!(PermName::is_writeable(PermName::PERM_READ | PermName::PERM_WRITE));
        assert!(PermName::is_writeable(
            PermName::PERM_READ | PermName::PERM_WRITE | PermName::PERM_PRIORITY
        ));
        assert!(PermName::is_writeable(
            PermName::PERM_READ | PermName::PERM_WRITE | PermName::PERM_PRIORITY | PermName::PERM_INHERIT
        ));
    }

    #[test]
    fn test_is_priority() {
        assert!(!PermName::is_priority(0));
        assert!(PermName::is_priority(PermName::PERM_PRIORITY));
        assert!(!PermName::is_priority(PermName::PERM_READ | PermName::PERM_WRITE));
        assert!(PermName::is_priority(
            PermName::PERM_READ | PermName::PERM_WRITE | PermName::PERM_PRIORITY
        ));
        assert!(PermName::is_priority(
            PermName::PERM_READ | PermName::PERM_WRITE | PermName::PERM_PRIORITY | PermName::PERM_INHERIT
        ));
    }

    #[test]
    fn test_is_inherit() {
        assert!(!PermName::is_inherited(0));
        assert!(PermName::is_inherited(PermName::PERM_INHERIT));
        assert!(!PermName::is_inherited(PermName::PERM_READ | PermName::PERM_WRITE));
        assert!(!PermName::is_inherited(
            PermName::PERM_READ | PermName::PERM_WRITE | PermName::PERM_PRIORITY
        ));
        assert!(PermName::is_inherited(
            PermName::PERM_READ | PermName::PERM_WRITE | PermName::PERM_PRIORITY | PermName::PERM_INHERIT
        ));
    }

    #[test]
    fn valid_str_returns_true_for_valid_permission() {
        assert!(PermName::is_valid_str("1"));
        assert!(PermName::is_valid_str("0"));
    }

    #[test]
    fn valid_str_returns_false_for_invalid_permission() {
        assert!(PermName::is_valid_str("4"));
        assert!(!PermName::is_valid_str("-1"));
    }

    #[test]
    fn valid_str_returns_false_for_non_numeric_input() {
        assert!(!PermName::is_valid_str("abc"));
        assert!(!PermName::is_valid_str(""));
    }
}
