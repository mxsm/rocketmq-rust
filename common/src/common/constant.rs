/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::ops::Deref;

pub struct PermName;
impl PermName {
    pub const INDEX_PERM_PRIORITY: i8 = 3;
    pub const INDEX_PERM_READ: i8 = 2;
    pub const INDEX_PERM_WRITE: i8 = 1;
    pub const INDEX_PERM_INHERIT: i8 = 0;

    pub const PERM_PRIORITY: i8 = 0x1 << PermName::INDEX_PERM_PRIORITY;
    pub const PERM_READ: i8 = 0x1 << PermName::INDEX_PERM_READ;
    pub const PERM_WRITE: i8 = 0x1 << PermName::INDEX_PERM_WRITE;
    pub const PERM_INHERIT: i8 = 0x1 << PermName::INDEX_PERM_INHERIT;

    pub fn perm_2_string(perm: i8) -> String {
        let mut simple = String::from("---");
        if Self::is_readable(perm) {
            simple.replace_range(0..1, "R");
        }
        if Self::is_writable(perm) {
            simple.replace_range(1..2, "W");
        }
        if Self::is_inherit(perm) {
            simple.replace_range(2..3, "X");
        }
        simple
    }
    pub fn is_readable(perm: i8) -> bool {
        perm & PermName::PERM_READ == PermName::PERM_READ
    }

    pub fn is_writable(perm: i8) -> bool {
        perm & PermName::PERM_WRITE == PermName::PERM_WRITE
    }
    pub fn is_priority(perm: i8) -> bool {
        (perm & PermName::PERM_PRIORITY) == PermName::PERM_PRIORITY
    }
    pub fn is_inherit(perm: i8) -> bool {
        (perm & PermName::PERM_INHERIT) == PermName::PERM_INHERIT
    }
}

#[cfg(test)]
mod tests {
    use super::PermName;

    #[test]
    fn test_perm_2_string() {
        assert_eq!(PermName::perm_2_string(0).as_str(), "---");
        assert_eq!(PermName::perm_2_string(PermName::PERM_READ).as_str(), "R--");
        assert_eq!(
            PermName::perm_2_string(PermName::PERM_READ | PermName::PERM_WRITE).as_str(),
            "RW-"
        );

        assert_eq!(
            PermName::perm_2_string(
                PermName::PERM_READ | PermName::PERM_WRITE | PermName::PERM_INHERIT
            ),
            "RWX"
        );
    }

    #[test]
    fn test_is_readable() {
        assert!(!PermName::is_readable(0));
        assert!(PermName::is_readable(PermName::PERM_READ));
        assert!(PermName::is_readable(
            PermName::PERM_READ | PermName::PERM_WRITE
        ));
        assert!(PermName::is_readable(
            PermName::PERM_READ | PermName::PERM_WRITE | PermName::PERM_PRIORITY
        ));
        assert!(PermName::is_readable(
            PermName::PERM_READ
                | PermName::PERM_WRITE
                | PermName::PERM_PRIORITY
                | PermName::PERM_INHERIT
        ));
    }

    #[test]
    fn test_is_writable() {
        assert!(!PermName::is_writable(0));
        assert!(PermName::is_writable(PermName::PERM_WRITE));
        assert!(PermName::is_writable(
            PermName::PERM_READ | PermName::PERM_WRITE
        ));
        assert!(PermName::is_writable(
            PermName::PERM_READ | PermName::PERM_WRITE | PermName::PERM_PRIORITY
        ));
        assert!(PermName::is_writable(
            PermName::PERM_READ
                | PermName::PERM_WRITE
                | PermName::PERM_PRIORITY
                | PermName::PERM_INHERIT
        ));
    }

    #[test]
    fn test_is_priority() {
        assert!(!PermName::is_priority(0));
        assert!(PermName::is_priority(PermName::PERM_PRIORITY));
        assert!(!PermName::is_priority(
            PermName::PERM_READ | PermName::PERM_WRITE
        ));
        assert!(PermName::is_priority(
            PermName::PERM_READ | PermName::PERM_WRITE | PermName::PERM_PRIORITY
        ));
        assert!(PermName::is_priority(
            PermName::PERM_READ
                | PermName::PERM_WRITE
                | PermName::PERM_PRIORITY
                | PermName::PERM_INHERIT
        ));
    }

    #[test]
    fn test_is_inherit() {
        assert!(!PermName::is_inherit(0));
        assert!(PermName::is_inherit(PermName::PERM_INHERIT));
        assert!(!PermName::is_inherit(
            PermName::PERM_READ | PermName::PERM_WRITE
        ));
        assert!(!PermName::is_inherit(
            PermName::PERM_READ | PermName::PERM_WRITE | PermName::PERM_PRIORITY
        ));
        assert!(PermName::is_inherit(
            PermName::PERM_READ
                | PermName::PERM_WRITE
                | PermName::PERM_PRIORITY
                | PermName::PERM_INHERIT
        ));
    }
}
