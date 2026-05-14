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

//! Java-compatible legacy ACL permission bits and migration helpers.

use std::fmt;

use rocketmq_common::common::action::Action;
use serde::Deserialize;
use serde::Serialize;

use crate::authorization::enums::decision::Decision;

#[derive(Clone, Copy, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Permission(u8);

impl Permission {
    pub const DENY: Self = Self(1);
    pub const ANY: Self = Self(1 << 1);
    pub const PUB: Self = Self(1 << 2);
    pub const SUB: Self = Self(1 << 3);

    pub fn bits(self) -> u8 {
        self.0
    }

    pub fn parse(value: Option<&str>) -> Self {
        match value.map(str::trim).unwrap_or_default() {
            "PUB" => Self::PUB,
            "SUB" => Self::SUB,
            "PUB|SUB" | "SUB|PUB" => Self(Self::PUB.0 | Self::SUB.0),
            "DENY" | "" => Self::DENY,
            _ => Self::DENY,
        }
    }

    pub fn contains(self, permission: Self) -> bool {
        self.0 & permission.0 == permission.0
    }

    pub fn migration_actions_and_decision(value: Option<&str>) -> (Vec<Action>, Decision) {
        let trimmed = value.map(str::trim).unwrap_or_default();
        let decision = if trimmed.is_empty() || trimmed == "DENY" {
            Decision::Deny
        } else {
            Decision::Allow
        };
        let actions = match trimmed {
            "PUB" => vec![Action::Pub],
            "SUB" => vec![Action::Sub],
            "PUB|SUB" | "SUB|PUB" => vec![Action::Pub, Action::Sub],
            "DENY" | "" => vec![Action::All],
            _ => vec![Action::All],
        };
        (actions, decision)
    }
}

impl fmt::Debug for Permission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Permission").field(&self.0).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_java_legacy_permission_bits() {
        assert_eq!(Permission::parse(Some("DENY")).bits(), 1);
        assert_eq!(Permission::parse(None).bits(), 1);
        assert_eq!(Permission::parse(Some("")).bits(), 1);
        assert_eq!(Permission::parse(Some("PUB")).bits(), 1 << 2);
        assert_eq!(Permission::parse(Some("SUB")).bits(), 1 << 3);
        assert_eq!(Permission::parse(Some("PUB|SUB")).bits(), (1 << 2) | (1 << 3));
        assert_eq!(Permission::parse(Some("SUB|PUB")).bits(), (1 << 2) | (1 << 3));
        assert_eq!(Permission::parse(Some("UNKNOWN")).bits(), 1);
    }

    #[test]
    fn combined_permission_contains_each_action_bit() {
        let permission = Permission::parse(Some("PUB|SUB"));

        assert!(permission.contains(Permission::PUB));
        assert!(permission.contains(Permission::SUB));
        assert!(!permission.contains(Permission::ANY));
    }

    #[test]
    fn maps_legacy_permission_to_java_migration_actions_and_decision() {
        assert_eq!(
            Permission::migration_actions_and_decision(Some("PUB")),
            (vec![Action::Pub], Decision::Allow)
        );
        assert_eq!(
            Permission::migration_actions_and_decision(Some("SUB")),
            (vec![Action::Sub], Decision::Allow)
        );
        assert_eq!(
            Permission::migration_actions_and_decision(Some("PUB|SUB")),
            (vec![Action::Pub, Action::Sub], Decision::Allow)
        );
        assert_eq!(
            Permission::migration_actions_and_decision(Some("SUB|PUB")),
            (vec![Action::Pub, Action::Sub], Decision::Allow)
        );
        assert_eq!(
            Permission::migration_actions_and_decision(Some("DENY")),
            (vec![Action::All], Decision::Deny)
        );
        assert_eq!(
            Permission::migration_actions_and_decision(None),
            (vec![Action::All], Decision::Deny)
        );
        assert_eq!(
            Permission::migration_actions_and_decision(Some("UNKNOWN")),
            (vec![Action::All], Decision::Allow)
        );
    }
}
