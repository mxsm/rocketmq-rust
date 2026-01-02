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

use std::convert::TryFrom;
use std::fmt;

/// Action represents the operations that can be authorized.
/// Mirrors the Java `Action` enum:
/// UNKNOWN(0, "Unknown"), ALL(1, "All"), ANY(2, "Any"), PUB(3, "Pub"),
/// SUB(4, "Sub"), CREATE(5, "Create"), UPDATE(6, "Update"), DELETE(7, "Delete"),
/// GET(8, "Get"), LIST(9, "List").
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    Unknown = 0,
    All = 1,
    Any = 2,
    Pub = 3,
    Sub = 4,
    Create = 5,
    Update = 6,
    Delete = 7,
    Get = 8,
    List = 9,
}

impl Action {
    /// Return the numeric code associated with the action.
    pub fn code(self) -> u8 {
        self as u8
    }

    /// Return the display name of the action.
    pub fn name(self) -> &'static str {
        match self {
            Action::Unknown => "Unknown",
            Action::All => "All",
            Action::Any => "Any",
            Action::Pub => "Pub",
            Action::Sub => "Sub",
            Action::Create => "Create",
            Action::Update => "Update",
            Action::Delete => "Delete",
            Action::Get => "Get",
            Action::List => "List",
        }
    }

    /// Case-insensitive lookup by name. Returns `None` if no match.
    pub fn get_by_name(name: &str) -> Option<Self> {
        match name.trim().to_ascii_lowercase().as_str() {
            "unknown" => Some(Action::Unknown),
            "all" => Some(Action::All),
            "any" => Some(Action::Any),
            "pub" => Some(Action::Pub),
            "sub" => Some(Action::Sub),
            "create" => Some(Action::Create),
            "update" => Some(Action::Update),
            "delete" => Some(Action::Delete),
            "get" => Some(Action::Get),
            "list" => Some(Action::List),
            _ => None,
        }
    }
}

impl TryFrom<u8> for Action {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Action::Unknown),
            1 => Ok(Action::All),
            2 => Ok(Action::Any),
            3 => Ok(Action::Pub),
            4 => Ok(Action::Sub),
            5 => Ok(Action::Create),
            6 => Ok(Action::Update),
            7 => Ok(Action::Delete),
            8 => Ok(Action::Get),
            9 => Ok(Action::List),
            _ => Err(()),
        }
    }
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::Action;

    #[test]
    fn test_action_code() {
        assert_eq!(Action::Unknown.code(), 0);
        assert_eq!(Action::All.code(), 1);
        assert_eq!(Action::Any.code(), 2);
        assert_eq!(Action::Pub.code(), 3);
        assert_eq!(Action::Sub.code(), 4);
        assert_eq!(Action::Create.code(), 5);
        assert_eq!(Action::Update.code(), 6);
        assert_eq!(Action::Delete.code(), 7);
        assert_eq!(Action::Get.code(), 8);
        assert_eq!(Action::List.code(), 9);
    }

    #[test]
    fn test_action_name() {
        assert_eq!(Action::Unknown.name(), "Unknown");
        assert_eq!(Action::All.name(), "All");
        assert_eq!(Action::Any.name(), "Any");
        assert_eq!(Action::Pub.name(), "Pub");
        assert_eq!(Action::Sub.name(), "Sub");
        assert_eq!(Action::Create.name(), "Create");
        assert_eq!(Action::Update.name(), "Update");
        assert_eq!(Action::Delete.name(), "Delete");
        assert_eq!(Action::Get.name(), "Get");
        assert_eq!(Action::List.name(), "List");
    }

    #[test]
    fn test_get_by_name_case_insensitive() {
        let test_cases = [
            ("unknown", Some(Action::Unknown)),
            ("UNKNOWN", Some(Action::Unknown)),
            ("aLL", Some(Action::All)),
            ("CrEaTe", Some(Action::Create)),
            ("  all  ", Some(Action::All)),
            ("\tpub\t", Some(Action::Pub)),
            (" sub ", Some(Action::Sub)),
            ("invalid", None),
            ("", None),
            ("publish", None),
        ];

        for (input, expected) in test_cases {
            assert_eq!(Action::get_by_name(input), expected, "input: {:?}", input);
        }
    }

    #[test]
    fn test_try_from_u8_valid() {
        assert_eq!(Action::try_from(0), Ok(Action::Unknown));
        assert_eq!(Action::try_from(1), Ok(Action::All));
        assert_eq!(Action::try_from(2), Ok(Action::Any));
        assert_eq!(Action::try_from(3), Ok(Action::Pub));
        assert_eq!(Action::try_from(4), Ok(Action::Sub));
        assert_eq!(Action::try_from(5), Ok(Action::Create));
        assert_eq!(Action::try_from(6), Ok(Action::Update));
        assert_eq!(Action::try_from(7), Ok(Action::Delete));
        assert_eq!(Action::try_from(8), Ok(Action::Get));
        assert_eq!(Action::try_from(9), Ok(Action::List));
    }

    #[test]
    fn test_try_from_u8_invalid() {
        assert_eq!(Action::try_from(10), Err(()));
        assert_eq!(Action::try_from(100), Err(()));
        assert_eq!(Action::try_from(255), Err(()));
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", Action::Unknown), "Unknown");
        assert_eq!(format!("{}", Action::All), "All");
        assert_eq!(format!("{}", Action::Any), "Any");
        assert_eq!(format!("{}", Action::Pub), "Pub");
        assert_eq!(format!("{}", Action::Sub), "Sub");
        assert_eq!(format!("{}", Action::Create), "Create");
        assert_eq!(format!("{}", Action::Update), "Update");
        assert_eq!(format!("{}", Action::Delete), "Delete");
        assert_eq!(format!("{}", Action::Get), "Get");
        assert_eq!(format!("{}", Action::List), "List");
    }
}
