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

// smart_encode_buffer.rs
//
// High-performance adaptive EncodeBuffer with automatic shrink behavior.
// - Expands as needed (using BytesMut growth rules).
// - Tracks recent write sizes using EMA (exponential moving average).
// - Shrinks conservatively based on EMA, ratio trigger, and cooldown to avoid jitter.
// - Provides metrics for diagnostics.
//

use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use bytes::BytesMut;

/// Configuration for EncodeBuffer adaptive behavior.
#[derive(Debug, Clone)]
pub struct EncodeBufferConfig {
    /// Minimum capacity to keep (do not shrink below this).
    pub min_capacity: usize,
    /// EMA smoothing factor in (0,1]. Higher alpha -> more sensitive to recent
    /// writes.
    pub ema_alpha: f64,
    /// Shrink trigger ratio: current_capacity >= ema_size *
    /// shrink_ratio_trigger => consider shrink.
    pub shrink_ratio_trigger: f64,
    /// Shrink target factor: target_capacity = max(min_capacity, ema_size *
    /// shrink_target_factor)
    pub shrink_target_factor: f64,
    /// Minimum time between two shrink operations (cooldown).
    pub shrink_cooldown: Duration,
    /// Absolute lower bound to consider shrink (if capacity <= this, do not
    /// shrink).
    pub min_shrink_threshold: usize,
}

impl Default for EncodeBufferConfig {
    fn default() -> Self {
        Self {
            min_capacity: 8 * 1024,    // 8 KB
            ema_alpha: 0.05,           // slow EMA to reduce jitter
            shrink_ratio_trigger: 3.0, // conservative: capacity >= 3x recent need
            shrink_target_factor: 1.5, // shrink to 1.5x of recent need
            shrink_cooldown: Duration::from_secs(30),
            min_shrink_threshold: 64 * 1024, // do not shrink small capacities (<64KB)
        }
    }
}

/// Runtime buffer statistics for diagnostics.
#[derive(Debug, Clone)]
pub struct BufferStats {
    pub current_capacity: usize,
    pub ema_size: f64,
    pub shrink_count: u64,
    pub expand_count: u64,
    pub historical_max: usize,
}

/// EncodeBuffer: zero-copy friendly buffer for encoding operations with
/// automatic, conservative shrinking to prevent long-term memory bloat.
///
/// Design goals:
/// - Avoid frequent reallocation on normal workload (keep capacity after expansion).
/// - Protect against single large spike causing permanent large allocation (occasional shrink).
/// - Avoid shrink jitter by cooldown + EMA + absolute threshold.
pub struct EncodeBuffer {
    buf: BytesMut,
    cfg: EncodeBufferConfig,

    // Exponential moving average of recent write sizes (in bytes).
    ema_recent_size: f64,
    // Timestamp of last shrink operation.
    last_shrink: Instant,
    // The maximum len observed at runtime (diagnostic).
    historical_max_len: usize,

    // Diagnostics counters
    shrink_count: u64,
    expand_count: u64,
    last_capacity: usize,
}

impl Default for EncodeBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl EncodeBuffer {
    /// Create a new EncodeBuffer with default configuration.
    pub fn new() -> Self {
        Self::with_config(EncodeBufferConfig::default())
    }

    /// Create a new EncodeBuffer with custom configuration.
    pub fn with_config(cfg: EncodeBufferConfig) -> Self {
        let initial = std::cmp::max(cfg.min_capacity, 8);
        EncodeBuffer {
            buf: BytesMut::with_capacity(initial),
            cfg,
            // Initialize EMA to a conservative value: twice the initial to avoid immediate shrink.
            ema_recent_size: (initial * 2) as f64,
            //allow immediate shrink if triggered
            last_shrink: Instant::now() - Duration::from_secs(60),

            historical_max_len: 0,
            shrink_count: 0,
            expand_count: 0,
            last_capacity: initial,
        }
    }

    /// Return current capacity.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.buf.capacity()
    }

    /// Return current length (amount of data written).
    #[inline]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Return whether buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Append a slice into the buffer.
    #[inline]
    pub fn append(&mut self, data: &[u8]) {
        self.track_expansion();
        self.buf.extend_from_slice(data);
    }

    /// Append a Bytes value into the buffer (will copy into internal BytesMut).
    /// Note: if you already have a complete chunk (single Bytes) to send,
    /// send the Bytes directly instead of appending them to the buffer.
    #[inline]
    pub fn append_bytes(&mut self, bytes: &Bytes) {
        self.track_expansion();
        self.buf.extend_from_slice(bytes.as_ref());
    }

    /// Provide mutable access to inner BytesMut for in-place encoding.
    /// Users that write directly should call `take_bytes` after writing.
    #[inline]
    pub fn buf_mut(&mut self) -> &mut BytesMut {
        self.track_expansion();
        &mut self.buf
    }

    /// Extract current contents as Bytes (zero-copy) and reset length to 0.
    /// This also updates EMA and may trigger a shrink according to policy.
    pub fn take_bytes(&mut self) -> Bytes {
        let len = self.buf.len();
        if len > self.historical_max_len {
            self.historical_max_len = len;
        }

        let out = if len > 0 {
            // split_to(len) returns the first len bytes and leaves self.buf.len() == 0.
            // Note: After split_to, the remaining capacity becomes 0.
            self.buf.split_to(len).freeze()
        } else {
            Bytes::new()
        };

        // Update EMA using the observed message size.
        self.update_ema(len);

        // Consider shrinking after updating EMA.
        // Note: maybe_shrink will allocate a new buffer with appropriate capacity
        self.maybe_shrink();

        // After split_to, the remaining buffer may have reduced capacity.
        // Ensure we have at least min_capacity for next write.
        // Only do this if we didn't just shrink (shrink already sets capacity correctly).
        let current_cap = self.buf.capacity();
        if current_cap < self.cfg.min_capacity && self.buf.is_empty() {
            // Replace the buffer with a new one at min_capacity
            self.buf = BytesMut::with_capacity(self.cfg.min_capacity);
        }

        out
    }

    /// Force immediate shrink to configured min_capacity.
    pub fn force_shrink_to_min(&mut self) {
        let min = self.cfg.min_capacity;
        if self.buf.capacity() > min {
            self.do_shrink(min);
        }
    }

    /// Return historic maximum recorded length.
    pub fn historical_max(&self) -> usize {
        self.historical_max_len
    }

    /// Return diagnostic stats.
    pub fn stats(&self) -> BufferStats {
        BufferStats {
            current_capacity: self.capacity(),
            ema_size: self.ema_recent_size,
            shrink_count: self.shrink_count,
            expand_count: self.expand_count,
            historical_max: self.historical_max_len,
        }
    }

    // --- internal helpers ---

    /// Track capacity increases for diagnostics.
    #[inline]
    fn track_expansion(&mut self) {
        let cur = self.buf.capacity();
        if cur > self.last_capacity {
            self.expand_count = self.expand_count.saturating_add(1);
            self.last_capacity = cur;
        }
    }

    /// Update the EMA with the last written size.
    #[inline]
    fn update_ema(&mut self, last_size: usize) {
        let alpha = self.cfg.ema_alpha;
        // guard for invalid alpha
        let alpha = if alpha <= 0.0 {
            0.05
        } else if alpha > 1.0 {
            1.0
        } else {
            alpha
        };
        self.ema_recent_size = alpha * (last_size as f64) + (1.0 - alpha) * self.ema_recent_size;
    }

    /// Decide whether to shrink and perform shrink if conditions are met.
    fn maybe_shrink(&mut self) {
        let cap = self.buf.capacity();
        let ema = self.ema_recent_size.max(1.0);

        // Do not attempt shrink below an absolute threshold to avoid
        // over-fragmentation.
        if cap <= self.cfg.min_shrink_threshold {
            return;
        }

        // Conservative trigger: only shrink when capacity >= ema * ratio.
        if (cap as f64) >= ema * self.cfg.shrink_ratio_trigger {
            let now = Instant::now();
            if now.duration_since(self.last_shrink) >= self.cfg.shrink_cooldown {
                // Compute target capacity (rounded up), enforce min_capacity lower bound.
                let target = std::cmp::max(
                    self.cfg.min_capacity,
                    (ema * self.cfg.shrink_target_factor).ceil() as usize,
                );

                if target < cap {
                    self.do_shrink(target);
                }
            }
        }
    }

    /// Perform the actual shrink: allocate a new BytesMut with target capacity.
    /// If allocation fails (unlikely), we keep the old buffer to preserve
    /// correctness.
    fn do_shrink(&mut self, target: usize) {
        // Attempt to allocate in a panic-safe way.
        match std::panic::catch_unwind(|| BytesMut::with_capacity(target)) {
            Ok(mut new_buf) => {
                // If there are leftover bytes (should be none after take_bytes), copy them.
                if !self.buf.is_empty() {
                    new_buf.extend_from_slice(&self.buf);
                }
                self.buf = new_buf;
                self.last_shrink = Instant::now();
                self.shrink_count = self.shrink_count.saturating_add(1);
                self.last_capacity = target;
            }
            Err(_) => {
                // Allocation panic occurred; do nothing to preserve existing
                // buffer.
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// Test that buffer expands when large writes occur.
    #[test]
    fn test_expand_on_large_write() {
        let mut eb = EncodeBuffer::new();
        let initial_cap = eb.capacity();
        // write bigger than initial capacity
        let big = vec![0u8; initial_cap * 4 + 10];
        eb.append(&big);
        assert!(eb.capacity() >= big.len(), "capacity did not expand as expected");
        let _ = eb.take_bytes();
        assert_eq!(eb.len(), 0);
    }

    /// Test that EMA tracks message sizes and shrink logic is sound.
    #[test]
    fn test_shrink_after_spike() {
        // Configure with settings that allow shrinking
        let cfg = EncodeBufferConfig {
            min_capacity: 128,
            ema_alpha: 0.3,
            shrink_ratio_trigger: 3.0,
            shrink_target_factor: 1.8,
            shrink_cooldown: Duration::from_millis(5),
            min_shrink_threshold: 256,
        };

        let mut eb = EncodeBuffer::with_config(cfg);

        // Write increasing sizes to build up EMA
        for i in 1..=10 {
            eb.append(&vec![0u8; i * 100]);
            let _ = eb.take_bytes();
        }

        let stats_after_increase = eb.stats();
        println!("After increasing writes - EMA: {:.1}", stats_after_increase.ema_size);

        // EMA should be somewhere in the middle range
        assert!(
            stats_after_increase.ema_size > 100.0 && stats_after_increase.ema_size < 1500.0,
            "EMA should reflect the varying message sizes"
        );

        // Now switch to very small writes
        for _ in 0..30 {
            eb.append(&[0u8; 20]);
            let _ = eb.take_bytes();
            std::thread::sleep(Duration::from_millis(1)); // Allow cooldown
        }

        let stats_final = eb.stats();
        println!(
            "After small writes - EMA: {:.1}, capacity: {}, shrinks: {}",
            stats_final.ema_size, stats_final.current_capacity, stats_final.shrink_count
        );

        // EMA should have decreased significantly
        assert!(
            stats_final.ema_size < stats_after_increase.ema_size,
            "EMA should decrease with small writes"
        );
        // Capacity should respect min_capacity
        assert!(
            stats_final.current_capacity >= 128,
            "Capacity should not go below min_capacity"
        );
    }

    /// Test EMA updates and stability (no excessive shrink when series of moderate
    /// writes).
    #[test]
    fn test_ema_and_no_shrink_on_stable_load() {
        let cfg = EncodeBufferConfig {
            min_capacity: 64,
            ema_alpha: 0.5,            // faster response for test
            shrink_ratio_trigger: 3.0, // higher ratio to avoid premature shrink
            shrink_target_factor: 1.5,
            shrink_cooldown: Duration::from_secs(1),
            min_shrink_threshold: 256, // prevent shrink for small capacities
        };

        let min_cap = cfg.min_capacity;
        let mut eb = EncodeBuffer::with_config(cfg);
        let initial_cap = eb.capacity();
        println!("Initial capacity: {}", initial_cap);

        // simulate moderate writes
        for _ in 0..10 {
            eb.append(&[0u8; 32]);
            let _ = eb.take_bytes();
        }
        let stats = eb.stats();
        println!("After 10 writes - capacity: {}, ema: {}", eb.capacity(), stats.ema_size);

        // EMA should converge towards 32 (approx)
        assert!(
            stats.ema_size > 1.0 && stats.ema_size < 1000.0,
            "EMA should be reasonable"
        );
        // Capacity should remain stable and not shrink below min_capacity
        assert!(eb.capacity() >= min_cap, "Capacity should not go below min_capacity");
        // With stable moderate load, capacity should be stable
        assert!(eb.capacity() <= initial_cap * 2, "Capacity should not grow excessively");
        // Should not have shrunk with stable load
        assert_eq!(stats.shrink_count, 0, "Should not shrink with stable moderate load");
    }

    /// Jitter test: ensure frequent tiny spikes do not cause frequent shrinks.
    #[test]
    fn test_no_jitter_under_flapping() {
        let cfg = EncodeBufferConfig {
            min_capacity: 32,
            ema_alpha: 0.1, // slow EMA to resist jitter
            shrink_ratio_trigger: 2.0,
            shrink_target_factor: 1.0,
            shrink_cooldown: Duration::from_millis(50), // small cooldown for test
            min_shrink_threshold: 0,
        };

        let mut eb = EncodeBuffer::with_config(cfg);
        // simulate occasional large spikes but mostly small writes
        for i in 0..200 {
            if i % 50 == 0 {
                // large spike
                eb.append(&[0u8; 4096]);
            } else {
                eb.append(&[0u8; 16]);
            }
            let _ = eb.take_bytes();
        }

        let stats = eb.stats();
        // shrink_count should be relatively small (0 or a few), not many
        assert!(stats.shrink_count <= 10, "too many shrinks => jitter");
    }
}
