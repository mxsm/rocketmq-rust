use serde::Deserialize;
use serde::Serialize;
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct OffsetOption {
    #[serde(rename = "type")]
    pub type_: OffsetOptionType,
    pub value: i64,
}

impl OffsetOption {
    pub const POLICY_LAST_VALUE: i64 = 0;
    pub const POLICY_MIN_VALUE: i64 = 1;
    pub const POLICY_MAX_VALUE: i64 = 2;

    #[must_use]
    #[inline]
    pub const fn new(type_: OffsetOptionType, value: i64) -> Self {
        Self { type_, value }
    }

    #[must_use]
    #[inline]
    pub const fn policy(policy: i64) -> Self {
        debug_assert!(
            policy == Self::POLICY_LAST_VALUE || policy == Self::POLICY_MIN_VALUE || policy == Self::POLICY_MAX_VALUE,
            "Invalid policy value"
        );
        Self {
            type_: OffsetOptionType::Policy,
            value: policy,
        }
    }

    #[must_use]
    #[inline]
    pub const fn offset(value: i64) -> Self {
        Self {
            type_: OffsetOptionType::Offset,
            value,
        }
    }

    #[must_use]
    #[inline]
    pub const fn tail_n(n: i64) -> Self {
        Self {
            type_: OffsetOptionType::TailN,
            value: n,
        }
    }

    #[must_use]
    #[inline]
    pub const fn timestamp(timestamp: i64) -> Self {
        Self {
            type_: OffsetOptionType::Timestamp,
            value: timestamp,
        }
    }

    #[inline]
    pub const fn type_(&self) -> OffsetOptionType {
        self.type_
    }

    #[inline]
    pub fn set_type(&mut self, type_: OffsetOptionType) {
        self.type_ = type_;
    }

    #[inline]
    pub const fn value(&self) -> i64 {
        self.value
    }

    #[inline]
    pub fn set_value(&mut self, value: i64) {
        self.value = value;
    }
}

impl Default for OffsetOption {
    fn default() -> Self {
        Self::policy(Self::POLICY_LAST_VALUE)
    }
}

impl fmt::Display for OffsetOption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OffsetOption {{ type: {:?}, value: {} }}", self.type_, self.value)
    }
}

/// Enumeration of offset seeking strategies.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[non_exhaustive]
pub enum OffsetOptionType {
    Policy = 0,
    Offset = 1,
    TailN = 2,
    Timestamp = 3,
}

impl OffsetOptionType {
    #[must_use]
    #[inline]
    pub const fn from_i32(value: i32) -> Option<Self> {
        match value {
            0 => Some(Self::Policy),
            1 => Some(Self::Offset),
            2 => Some(Self::TailN),
            3 => Some(Self::Timestamp),
            _ => None,
        }
    }

    #[must_use]
    #[inline]
    pub const fn as_i32(self) -> i32 {
        self as i32
    }
}

impl fmt::Display for OffsetOptionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Policy => f.write_str("POLICY"),
            Self::Offset => f.write_str("OFFSET"),
            Self::TailN => f.write_str("TAIL_N"),
            Self::Timestamp => f.write_str("TIMESTAMP"),
        }
    }
}
