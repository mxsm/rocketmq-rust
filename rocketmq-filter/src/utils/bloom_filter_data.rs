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

use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BloomFilterData {
    bit_pos: Vec<i32>,
    bit_num: u32,
}

impl BloomFilterData {
    pub fn new(bit_pos: Vec<i32>, bit_num: u32) -> Self {
        Self { bit_pos, bit_num }
    }

    pub fn set_bit_pos(&mut self, bit_pos: Vec<i32>) {
        self.bit_pos = bit_pos;
    }

    pub fn set_bit_num(&mut self, bit_num: u32) {
        self.bit_num = bit_num;
    }

    pub fn bit_pos(&self) -> &Vec<i32> {
        &self.bit_pos
    }

    pub fn bit_num(&self) -> u32 {
        self.bit_num
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_creates_bloom_filter_data_with_given_values() {
        let bit_pos = vec![1, 2, 3];
        let bit_num = 10;
        let bloom_filter_data = BloomFilterData::new(bit_pos.clone(), bit_num);

        assert_eq!(bloom_filter_data.bit_pos(), &bit_pos);
        assert_eq!(bloom_filter_data.bit_num(), bit_num);
    }

    #[test]
    fn set_bit_pos_updates_bit_pos() {
        let mut bloom_filter_data = BloomFilterData::new(vec![1, 2, 3], 10);
        let new_bit_pos = vec![4, 5, 6];

        bloom_filter_data.set_bit_pos(new_bit_pos.clone());

        assert_eq!(bloom_filter_data.bit_pos(), &new_bit_pos);
    }

    #[test]
    fn set_bit_num_updates_bit_num() {
        let mut bloom_filter_data = BloomFilterData::new(vec![1, 2, 3], 10);
        let new_bit_num = 20;

        bloom_filter_data.set_bit_num(new_bit_num);

        assert_eq!(bloom_filter_data.bit_num(), new_bit_num);
    }

    #[test]
    fn bit_pos_returns_bit_pos() {
        let bit_pos = vec![1, 2, 3];
        let bloom_filter_data = BloomFilterData::new(bit_pos.clone(), 10);

        assert_eq!(bloom_filter_data.bit_pos(), &bit_pos);
    }

    #[test]
    fn bit_num_returns_bit_num() {
        let bit_num = 10;
        let bloom_filter_data = BloomFilterData::new(vec![1, 2, 3], bit_num);

        assert_eq!(bloom_filter_data.bit_num(), bit_num);
    }
}
