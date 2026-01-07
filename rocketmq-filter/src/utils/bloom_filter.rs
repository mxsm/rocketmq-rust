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

use std::hash::Hasher;

use ahash::AHasher;
use rocketmq_error::FilterError;
use rocketmq_error::RocketMQResult;

use crate::utils::bits_array::BitsArray;
use crate::utils::bloom_filter_data::BloomFilterData;

/// Simple implementation of Bloom Filter.
///
/// This implementation follows the Java RocketMQ BloomFilter design:
/// - Uses double hashing technique (Kirsch-Mitzenmacher optimization)
/// - Configurable false positive rate
/// - Bit array size automatically calculated
///
/// # Examples
///
/// ```ignore
/// use rocketmq_filter::utils::bloom_filter::BloomFilter;
/// use rocketmq_filter::utils::bits_array::BitsArray;
///
/// let filter = BloomFilter::create_by_fn(10, 100).unwrap();
/// let mut bits = BitsArray::create(filter.m() as usize);
///
/// filter.hash_to_str("key1", &mut bits).unwrap();
/// assert!(filter.is_hit_str("key1", &bits).unwrap());
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BloomFilter {
    /// Error rate (f/100, e.g., 10 means 10%)
    f: i32,
    /// Expected number of elements
    n: i32,
    /// Number of hash functions
    k: i32,
    /// Total number of bits
    m: i32,
}

impl BloomFilter {
    /// Create a Bloom filter with specified error rate and expected element count.
    ///
    /// # Arguments
    ///
    /// * `f` - Error rate (1-99, representing percentage)
    /// * `n` - Expected number of elements
    ///
    /// # Formula
    ///
    /// - k = ceil(ln(0.5) / ln(error_rate))
    /// - m = ceil(n * ln(1/error_rate) / (ln(2)^2))
    /// - m adjusted to be multiple of 8
    pub fn create_by_fn(f: i32, n: i32) -> RocketMQResult<Self> {
        if !(1..100).contains(&f) {
            return Err(FilterError::invalid_bit_length().into());
        }
        if n < 1 {
            return Err(FilterError::invalid_bit_length().into());
        }

        let error_rate = f as f64 / 100.0;

        // Calculate optimal k: k = ln(2) * (m/n) ≈ 0.693 * (m/n)
        // Simplified: when p=0.5, k = ln(0.5) / ln(error_rate)
        let k = (Self::log_mn(0.5, error_rate)).ceil() as i32;

        if k < 1 {
            return Err(FilterError::invalid_bit_length().into());
        }

        // Calculate m: m = n * ln(1/error_rate) / (ln(2))^2
        let m = (n as f64 * Self::log_mn(2.0, 1.0 / error_rate) * Self::log_mn(2.0, std::f64::consts::E)).ceil() as i32;

        // Ensure m is multiple of 8 (byte alignment)
        let m = 8 * ((m + 7) / 8);

        Ok(BloomFilter { f, n, k, m })
    }

    /// Calculate bit positions for a string using double hashing.
    ///
    /// Implements the Kirsch-Mitzenmacher optimization:
    /// "Less Hashing, Same Performance: Building a Better Bloom Filter"
    ///
    /// Uses two hash values (h1, h2) to generate k hash positions:
    /// g(i) = h1 + i * h2 (mod m)
    pub fn calc_bit_positions(&self, s: &str) -> Vec<i32> {
        let mut bit_positions = vec![0i32; self.k as usize];

        // Use AHash for best performance and security
        let hash64 = Self::hash_string(s);

        let hash1 = hash64 as i32;
        let hash2 = (hash64 >> 32) as i32;

        for i in 1..=self.k {
            let mut combined_hash = hash1.wrapping_add(i.wrapping_mul(hash2));

            // Flip bits if negative to ensure positive
            if combined_hash < 0 {
                combined_hash = !combined_hash;
            }

            bit_positions[(i - 1) as usize] = combined_hash % self.m;
        }

        bit_positions
    }

    /// Generate BloomFilterData for a string.
    pub fn generate(&self, s: &str) -> BloomFilterData {
        let bit_positions = self.calc_bit_positions(s);
        BloomFilterData::new(bit_positions, self.m as u32)
    }

    /// Hash a string to bit array (by string).
    pub fn hash_to_str(&self, s: &str, bits: &mut BitsArray) -> RocketMQResult<()> {
        let bit_positions = self.calc_bit_positions(s);
        self.hash_to_positions(&bit_positions, bits)
    }

    /// Hash to bit array using pre-calculated positions.
    pub fn hash_to_positions(&self, bit_positions: &[i32], bits: &mut BitsArray) -> RocketMQResult<()> {
        self.check(bits)?;

        for &pos in bit_positions {
            bits.set_bit(pos as usize, true)?;
        }

        Ok(())
    }

    /// Hash to bit array using BloomFilterData.
    pub fn hash_to(&self, filter_data: &BloomFilterData, bits: &mut BitsArray) -> RocketMQResult<()> {
        if !self.is_valid(Some(filter_data)) {
            return Err(FilterError::bit_length_too_small().into());
        }

        self.hash_to_positions(filter_data.bit_pos(), bits)
    }

    /// Check if a string might be in the set (by string).
    pub fn is_hit_str(&self, s: &str, bits: &BitsArray) -> RocketMQResult<bool> {
        let bit_positions = self.calc_bit_positions(s);
        self.is_hit_positions(&bit_positions, bits)
    }

    /// Check if all bit positions are set.
    pub fn is_hit_positions(&self, bit_positions: &[i32], bits: &BitsArray) -> RocketMQResult<bool> {
        self.check(bits)?;

        // Check first position
        let mut result = bits.get_bit(bit_positions[0] as usize)?;

        // AND with remaining positions
        for &pos in &bit_positions[1..] {
            result &= bits.get_bit(pos as usize)?;
        }

        Ok(result)
    }

    /// Check if BloomFilterData might be in the set.
    pub fn is_hit(&self, filter_data: &BloomFilterData, bits: &BitsArray) -> RocketMQResult<bool> {
        if !self.is_valid(Some(filter_data)) {
            return Err(FilterError::bit_length_too_small().into());
        }

        self.is_hit_positions(filter_data.bit_pos(), bits)
    }

    /// Check if positions would result in a false positive.
    ///
    /// Returns true if all positions are already occupied.
    pub fn check_false_hit(&self, bit_positions: &[i32], bits: &BitsArray) -> RocketMQResult<bool> {
        for &pos in bit_positions {
            if !bits.get_bit(pos as usize)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Validate bit array length matches filter configuration.
    #[inline]
    fn check(&self, bits: &BitsArray) -> RocketMQResult<()> {
        if bits.bit_length() != self.m as usize {
            return Err(FilterError::bit_length_too_small().into());
        }
        Ok(())
    }

    /// Validate BloomFilterData belongs to this filter.
    pub fn is_valid(&self, filter_data: Option<&BloomFilterData>) -> bool {
        match filter_data {
            Some(data) => {
                data.bit_num() == self.m as u32 && !data.bit_pos().is_empty() && data.bit_pos().len() == self.k as usize
            }
            None => false,
        }
    }

    /// Hash string using AHash (fastest Rust hash, DoS resistant).
    #[inline]
    fn hash_string(s: &str) -> u64 {
        let mut hasher = AHasher::default();
        hasher.write(s.as_bytes());
        hasher.finish()
    }

    /// Calculate log base m of n: log_m(n)
    #[inline]
    fn log_mn(m: f64, n: f64) -> f64 {
        n.ln() / m.ln()
    }

    // Getters

    /// Get error rate (percentage).
    #[inline]
    pub fn f(&self) -> i32 {
        self.f
    }

    /// Get expected element count.
    #[inline]
    pub fn n(&self) -> i32 {
        self.n
    }

    /// Get number of hash functions.
    #[inline]
    pub fn k(&self) -> i32 {
        self.k
    }

    /// Get total number of bits.
    #[inline]
    pub fn m(&self) -> i32 {
        self.m
    }
}

impl std::fmt::Display for BloomFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BloomFilter {{ f: {}, n: {}, k: {}, m: {} }}",
            self.f, self.n, self.k, self.m
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_by_fn() {
        let filter = BloomFilter::create_by_fn(10, 100).unwrap();
        assert_eq!(filter.f(), 10);
        assert_eq!(filter.n(), 100);
        assert!(filter.k() > 0);
        assert!(filter.m() > 0);
        assert_eq!(filter.m() % 8, 0); // Must be multiple of 8
    }

    #[test]
    fn test_invalid_parameters() {
        assert!(BloomFilter::create_by_fn(0, 100).is_err());
        assert!(BloomFilter::create_by_fn(100, 100).is_err());
        assert!(BloomFilter::create_by_fn(10, 0).is_err());
    }

    #[test]
    fn test_calc_bit_positions() {
        let filter = BloomFilter::create_by_fn(10, 100).unwrap();
        let positions = filter.calc_bit_positions("test_key");

        assert_eq!(positions.len(), filter.k() as usize);

        // All positions should be within bounds
        for &pos in &positions {
            assert!(pos >= 0 && pos < filter.m());
        }

        // Same input should produce same positions
        let positions2 = filter.calc_bit_positions("test_key");
        assert_eq!(positions, positions2);
    }

    #[test]
    fn test_hash_and_hit() {
        let filter = BloomFilter::create_by_fn(10, 100).unwrap();
        let mut bits = BitsArray::create(filter.m() as usize);

        // Hash a key
        filter.hash_to_str("key1", &mut bits).unwrap();

        // Should hit
        assert!(filter.is_hit_str("key1", &bits).unwrap());

        // Should not hit (most likely)
        assert!(!filter.is_hit_str("key2", &bits).unwrap());
    }

    #[test]
    fn test_multiple_keys() {
        let filter = BloomFilter::create_by_fn(5, 200).unwrap();
        let mut bits = BitsArray::create(filter.m() as usize);

        let keys = vec!["key1", "key2", "key3", "key4", "key5"];

        // Add all keys
        for key in &keys {
            filter.hash_to_str(key, &mut bits).unwrap();
        }

        // All keys should hit
        for key in &keys {
            assert!(filter.is_hit_str(key, &bits).unwrap());
        }
    }

    #[test]
    fn test_generate() {
        let filter = BloomFilter::create_by_fn(10, 100).unwrap();
        let data = filter.generate("test_key");

        assert_eq!(data.bit_num(), filter.m() as u32);
        assert_eq!(data.bit_pos().len(), filter.k() as usize);
        assert!(filter.is_valid(Some(&data)));
    }

    #[test]
    fn test_is_valid() {
        let filter = BloomFilter::create_by_fn(10, 100).unwrap();

        // Valid data
        let valid_data = BloomFilterData::new(vec![1, 2, 3], filter.m() as u32);
        if valid_data.bit_pos().len() == filter.k() as usize {
            assert!(filter.is_valid(Some(&valid_data)));
        }

        // Invalid: wrong bit count
        let invalid_data = BloomFilterData::new(vec![1, 2, 3], (filter.m() + 1) as u32);
        assert!(!filter.is_valid(Some(&invalid_data)));

        // None
        assert!(!filter.is_valid(None));
    }

    #[test]
    fn test_check_false_hit() {
        let filter = BloomFilter::create_by_fn(1, 300).unwrap();
        let mut bits = BitsArray::create(filter.m() as usize);

        let mut false_hits = 0;

        for i in 0..filter.n() {
            let key = format!("key_{}", i);
            let positions = filter.calc_bit_positions(&key);

            if filter.check_false_hit(&positions, &bits).unwrap() {
                false_hits += 1;
            }

            filter.hash_to_positions(&positions, &mut bits).unwrap();
        }

        // False positive rate should be within expected range
        let actual_rate = (false_hits as f64 / filter.n() as f64) * 100.0;
        assert!(actual_rate <= filter.f() as f64);
    }

    #[test]
    fn test_equality() {
        let filter1 = BloomFilter::create_by_fn(10, 100).unwrap();
        let filter2 = BloomFilter::create_by_fn(10, 100).unwrap();
        let filter3 = BloomFilter::create_by_fn(20, 100).unwrap();

        assert_eq!(filter1, filter2);
        assert_ne!(filter1, filter3);
    }
}
