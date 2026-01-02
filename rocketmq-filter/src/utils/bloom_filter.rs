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

use crate::utils::bits_array::BitsArray;
use crate::utils::bloom_filter_data::BloomFilterData;

#[derive(Clone, Copy)]
pub struct BloomFilter {
    // as error rate, 10/100 = 0.1
    f: i32,
    n: i32,
    // hash function num, by calculation.
    k: i32,
    // bit count, by calculation.
    m: i32,
}

impl Default for BloomFilter {
    fn default() -> Self {
        BloomFilter {
            f: 10,
            n: 128,
            k: 0,
            m: 0,
        }
    }
}

#[allow(unused_mut)]
#[allow(unused_variables)]
impl BloomFilter {
    pub fn new(f: i32, n: i32) -> Result<Self, &'static str> {
        if !(1..100).contains(&f) {
            return Err("f must be greater or equal than 1 and less than 100");
        }
        if n < 1 {
            return Err("n must be greater than 0");
        }

        let error_rate = f as f64 / 100.0;
        let k = (0.5f64.log2() * (n as f64 * error_rate.ln()).abs() / error_rate.ln()).ceil() as i32;

        if k < 1 {
            return Err(
                "Hash function num is less than 1, maybe you should change the value of error rate or bit num!",
            );
        }

        let m = (n as f64 * error_rate.ln().abs() / (2f64.ln() * 2f64.ln())).ceil() as i32;
        let m = 8 * ((m + 7) / 8); // Ensure m is a multiple of 8

        Ok(BloomFilter { f, n, k, m })
    }

    pub fn f(&self) -> i32 {
        self.f
    }

    pub fn n(&self) -> i32 {
        self.n
    }

    pub fn k(&self) -> i32 {
        self.k
    }

    pub fn m(&self) -> i32 {
        self.m
    }

    pub fn is_valid(&self, filter_data: Option<&BloomFilterData>) -> bool {
        match filter_data {
            Some(data) => data.bit_num() == self.m as u32 && data.bit_pos().len() == self.k as usize,
            None => false,
        }
    }

    pub fn hash_to(&self, filter_data: &BloomFilterData, bits: &mut BitsArray) {
        /* if !self.is_valid(filter_data) {
            panic!(
                "Bloom filter data may not belong to this filter! {:?}, {:?}",
                filter_data, self
            );
        }
        self.hash_to_positions(filter_data.bit_pos(), bits);*/
        unimplemented!("hash_to");
    }

    // Helper method for setting bits at given positions
    pub fn hash_to_positions(&self, bit_positions: &[usize], bits: &mut BitsArray) {
        /*self.check(bits);
        for &i in bit_positions {
            bits.set_bit(i, true);
        }*/
        unimplemented!("hash_to_positions");
    }
}
