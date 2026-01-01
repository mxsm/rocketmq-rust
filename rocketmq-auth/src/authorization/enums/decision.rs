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

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(u8)]
pub enum Decision {
    Allow = 1,
    Deny = 2,
}

impl Decision {
    pub fn code(self) -> u8 {
        self as u8
    }

    pub fn name(self) -> &'static str {
        match self {
            Decision::Allow => "Allow",
            Decision::Deny => "Deny",
        }
    }

    pub fn get_by_name(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case("Allow") {
            Some(Decision::Allow)
        } else if name.eq_ignore_ascii_case("Deny") {
            Some(Decision::Deny)
        } else {
            None
        }
    }
}

impl From<Decision> for u8 {
    fn from(d: Decision) -> Self {
        d.code()
    }
}

impl TryFrom<u8> for Decision {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Decision::Allow),
            2 => Ok(Decision::Deny),
            _ => Err(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decision_code() {
        assert_eq!(Decision::Allow.code(), 1);
        assert_eq!(Decision::Deny.code(), 2);
    }

    #[test]
    fn test_decision_name() {
        assert_eq!(Decision::Allow.name(), "Allow");
        assert_eq!(Decision::Deny.name(), "Deny");
    }

    #[test]
    fn test_decision_get_by_name() {
        assert_eq!(Decision::get_by_name("Allow"), Some(Decision::Allow));
        assert_eq!(Decision::get_by_name("allow"), Some(Decision::Allow));
        assert_eq!(Decision::get_by_name("Deny"), Some(Decision::Deny));
        assert_eq!(Decision::get_by_name("deny"), Some(Decision::Deny));
        assert_eq!(Decision::get_by_name("Unknown"), None);
    }

    #[test]
    fn test_decision_from_u8() {
        assert_eq!(u8::from(Decision::Allow), 1);
        assert_eq!(u8::from(Decision::Deny), 2);
    }

    #[test]
    fn test_decision_try_from_u8() {
        assert_eq!(Decision::try_from(1), Ok(Decision::Allow));
        assert_eq!(Decision::try_from(2), Ok(Decision::Deny));
        assert_eq!(Decision::try_from(3), Err(()));
    }
}
