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

/// Represents a slot of timing wheel. Format:
/// ┌────────────┬───────────┬───────────┬───────────┬───────────┐
/// │delayed time│ first pos │ last pos  │    num    │   magic   │
/// ├────────────┼───────────┼───────────┼───────────┼───────────┤
/// │   8bytes   │   8bytes  │  8bytes   │   4bytes  │   4bytes  │
/// └────────────┴───────────┴───────────┴───────────┴───────────┘
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Slot {
    pub time_ms: i64, // delayed time
    pub first_pos: i64,
    pub last_pos: i64,
    pub num: i32,
    pub magic: i32, // no use now, just keep it
}

impl Slot {
    pub const SIZE: i16 = 32;

    pub fn new(time_ms: i64, first_pos: i64, last_pos: i64) -> Self {
        Slot {
            time_ms,
            first_pos,
            last_pos,
            num: 0,
            magic: 0,
        }
    }

    pub fn new_with_num_magic(time_ms: i64, first_pos: i64, last_pos: i64, num: i32, magic: i32) -> Self {
        Slot {
            time_ms,
            first_pos,
            last_pos,
            num,
            magic,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_slot() -> Slot {
        Slot::new(1000, 10, 20)
    }

    #[test]
    fn creates_slot_with_default_num_and_magic() {
        let slot = Slot::new(1000, 10, 20);

        assert_eq!(slot.time_ms, 1000);
        assert_eq!(slot.first_pos, 10);
        assert_eq!(slot.last_pos, 20);
        assert_eq!(slot.num, 0);
        assert_eq!(slot.magic, 0);
    }

    #[test]
    fn creates_slot_with_custom_num_and_magic() {
        let slot = Slot::new_with_num_magic(2000, 30, 40, 5, 99);

        assert_eq!(slot.time_ms, 2000);
        assert_eq!(slot.first_pos, 30);
        assert_eq!(slot.last_pos, 40);
        assert_eq!(slot.num, 5);
        assert_eq!(slot.magic, 99);
    }

    #[test]
    fn slot_size_constant_is_correct() {
        assert_eq!(Slot::SIZE, 32);
    }

    #[test]
    fn slots_with_same_values_are_equal() {
        let slot1 = create_slot();
        let slot2 = Slot::new(1000, 10, 20);

        assert_eq!(slot1, slot2);
    }

    #[test]
    fn slots_with_different_values_are_not_equal() {
        let slot1 = create_slot();
        let slot2 = Slot::new_with_num_magic(2000, 30, 40, 5, 99);

        assert_ne!(slot1, slot2);
    }
}
