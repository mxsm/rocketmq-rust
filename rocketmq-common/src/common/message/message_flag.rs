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

/// Message flags as a type-safe bitflag.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MessageFlag(i32);

impl MessageFlag {
    pub const NONE: Self = Self(0);
    pub const COMPRESSED: Self = Self(1 << 0);
    pub const MULTI_TAGS: Self = Self(1 << 1);
    pub const TRANSACTION_PREPARED: Self = Self(1 << 2);
    pub const TRANSACTION_COMMIT: Self = Self(1 << 3);
    pub const TRANSACTION_ROLLBACK: Self = Self(1 << 4);

    /// Creates an empty flag (no bits set).
    #[inline]
    pub const fn empty() -> Self {
        Self(0)
    }

    /// Creates a new flag from raw value.
    #[inline]
    pub const fn from_bits(bits: i32) -> Self {
        Self(bits)
    }

    /// Returns the raw flag value.
    #[inline]
    pub const fn bits(&self) -> i32 {
        self.0
    }

    /// Checks if a flag is set.
    #[inline]
    pub const fn contains(&self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    /// Sets a flag.
    #[inline]
    pub fn insert(&mut self, other: Self) {
        self.0 |= other.0;
    }

    /// Removes a flag.
    #[inline]
    pub fn remove(&mut self, other: Self) {
        self.0 &= !other.0;
    }

    /// Returns true if the compressed flag is set.
    #[inline]
    pub fn is_compressed(&self) -> bool {
        self.contains(Self::COMPRESSED)
    }

    /// Returns true if the multi-tags flag is set.
    #[inline]
    pub fn is_multi_tags(&self) -> bool {
        self.contains(Self::MULTI_TAGS)
    }

    /// Returns true if the transaction prepared flag is set.
    #[inline]
    pub fn is_transaction_prepared(&self) -> bool {
        self.contains(Self::TRANSACTION_PREPARED)
    }

    /// Returns true if the transaction commit flag is set.
    #[inline]
    pub fn is_transaction_commit(&self) -> bool {
        self.contains(Self::TRANSACTION_COMMIT)
    }

    /// Returns true if the transaction rollback flag is set.
    #[inline]
    pub fn is_transaction_rollback(&self) -> bool {
        self.contains(Self::TRANSACTION_ROLLBACK)
    }
}

impl std::ops::BitOr for MessageFlag {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl std::ops::BitOrAssign for MessageFlag {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

impl std::ops::BitAnd for MessageFlag {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        Self(self.0 & rhs.0)
    }
}

impl std::ops::BitAndAssign for MessageFlag {
    fn bitand_assign(&mut self, rhs: Self) {
        self.0 &= rhs.0;
    }
}

impl From<i32> for MessageFlag {
    fn from(value: i32) -> Self {
        Self(value)
    }
}

impl From<MessageFlag> for i32 {
    fn from(flag: MessageFlag) -> Self {
        flag.0
    }
}
