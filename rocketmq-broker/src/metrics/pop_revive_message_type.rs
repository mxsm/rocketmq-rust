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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
/// Represents the type of Pop Revive message used by the broker metrics system.
///
/// Variants:
/// - `Ck`: Checkpoint (CK) indicator.
/// - `Ack`: Acknowledgement (ACK) indicator.
pub enum PopReviveMessageType {
    Ck,
    Ack,
}
impl PopReviveMessageType {
    /// Returns the short, uppercase string representation of the message type.
    ///
    /// This is used for metrics labeling and serialization where compact identifiers
    /// are preferred.
    ///
    /// Returns:
    /// - "CK" for [`PopReviveMessageType::Ck`]
    /// - "ACK" for [`PopReviveMessageType::Ack`]
    pub fn as_str(&self) -> &'static str {
        match self {
            PopReviveMessageType::Ck => "CK",
            PopReviveMessageType::Ack => "ACK",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PopReviveMessageType;

    #[test]
    fn as_str_returns_ck_for_ck_variant() {
        let t = PopReviveMessageType::Ck;
        assert_eq!(t.as_str(), "CK");
    }

    #[test]
    fn as_str_returns_ack_for_ack_variant() {
        let t = PopReviveMessageType::Ack;
        assert_eq!(t.as_str(), "ACK");
    }

    #[test]
    fn enum_variants_are_distinct_and_comparable() {
        assert_ne!(PopReviveMessageType::Ck, PopReviveMessageType::Ack);
        assert_eq!(PopReviveMessageType::Ck, PopReviveMessageType::Ck);
    }

    #[test]
    fn as_str_output_is_static_and_can_be_used_as_key() {
        let s_ck: &'static str = PopReviveMessageType::Ck.as_str();
        let s_ack: &'static str = PopReviveMessageType::Ack.as_str();
        let mut counts = std::collections::HashMap::new();
        counts.insert(s_ck, 1usize);
        counts.insert(s_ack, 2usize);
        assert_eq!(counts.get("CK"), Some(&1));
        assert_eq!(counts.get("ACK"), Some(&2));
    }
}
