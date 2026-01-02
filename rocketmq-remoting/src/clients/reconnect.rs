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

//! Auto-reconnection mechanism with exponential backoff and circuit breaker.
//!
//! This module provides robust reconnection logic for transient network failures,
//! preventing resource exhaustion while maintaining high availability.

use std::time::Duration;
use std::time::Instant;

/// Exponential backoff strategy for connection retries.
///
/// # Algorithm
///
/// ```text
/// delay = min(initial * 2^attempt, max)
///
/// Example (initial=1s, max=60s):
///   Attempt 0: 1s
///   Attempt 1: 2s
///   Attempt 2: 4s
///   Attempt 3: 8s
///   Attempt 4: 16s
///   Attempt 5: 32s
///   Attempt 6: 60s (capped)
/// ```
#[derive(Debug, Clone)]
pub struct ExponentialBackoff {
    /// Initial retry delay
    initial_delay: Duration,

    /// Maximum retry delay (prevents unbounded growth)
    max_delay: Duration,

    /// Maximum number of retry attempts (0 = unlimited)
    max_attempts: u32,

    /// Current attempt number
    current_attempt: u32,
}

impl ExponentialBackoff {
    /// Creates a new exponential backoff strategy.
    ///
    /// # Arguments
    ///
    /// * `initial_delay` - First retry delay (e.g., 1 second)
    /// * `max_delay` - Maximum retry delay (e.g., 60 seconds)
    /// * `max_attempts` - Maximum retry attempts (0 = unlimited)
    ///
    /// # Example
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use rocketmq_remoting::clients::reconnect::ExponentialBackoff;
    ///
    /// let backoff = ExponentialBackoff::new(Duration::from_secs(1), Duration::from_secs(60), 10);
    /// ```
    pub fn new(initial_delay: Duration, max_delay: Duration, max_attempts: u32) -> Self {
        Self {
            initial_delay,
            max_delay,
            max_attempts,
            current_attempt: 0,
        }
    }

    /// Creates a default backoff strategy (1s initial, 60s max, unlimited attempts).
    pub fn default_strategy() -> Self {
        Self::new(Duration::from_secs(1), Duration::from_secs(60), 0)
    }

    /// Calculates the next retry delay.
    ///
    /// # Returns
    ///
    /// * `Some(duration)` - Retry delay for current attempt
    /// * `None` - Max attempts reached, should give up
    pub fn next_delay(&mut self) -> Option<Duration> {
        // Check if max attempts exceeded
        if self.max_attempts > 0 && self.current_attempt >= self.max_attempts {
            return None;
        }

        // Calculate exponential delay: initial * 2^attempt
        let delay_secs = self.initial_delay.as_secs() * 2u64.pow(self.current_attempt);
        let delay = Duration::from_secs(delay_secs).min(self.max_delay);

        self.current_attempt += 1;
        Some(delay)
    }

    /// Resets the backoff state after successful connection.
    pub fn reset(&mut self) {
        self.current_attempt = 0;
    }

    /// Gets the current attempt number.
    pub fn current_attempt(&self) -> u32 {
        self.current_attempt
    }

    /// Checks if max attempts reached.
    pub fn is_exhausted(&self) -> bool {
        self.max_attempts > 0 && self.current_attempt >= self.max_attempts
    }
}

/// Circuit breaker pattern for preventing cascading failures.
///
/// # States
///
/// ```text
/// ┌──────────┐      failure threshold       ┌──────────┐
/// │  CLOSED  │ ─────────────────────────► │   OPEN   │
/// └──────────┘                             └──────────┘
///      ▲                                        │
///      │                                        │ timeout
///      │                                        ▼
///      │         success threshold        ┌──────────┐
///      └────────────────────────────────  │ HALF_OPEN│
///                                         └──────────┘
/// ```
///
/// - **CLOSED**: Normal operation, requests pass through
/// - **OPEN**: Too many failures, reject requests immediately
/// - **HALF_OPEN**: Testing if service recovered, allow limited requests
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation, all requests allowed
    Closed,

    /// Service failing, reject requests to prevent overload
    Open,

    /// Testing recovery, allow probe requests
    HalfOpen,
}

/// Circuit breaker for connection management.
#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    /// Current circuit state
    state: CircuitState,

    /// Consecutive failure count in CLOSED state
    failure_count: u32,

    /// Failure threshold to trip circuit (CLOSED → OPEN)
    failure_threshold: u32,

    /// Success threshold to close circuit (HALF_OPEN → CLOSED)
    success_threshold: u32,

    /// Consecutive success count in HALF_OPEN state
    success_count: u32,

    /// Duration to wait before transitioning OPEN → HALF_OPEN
    timeout: Duration,

    /// Timestamp when circuit opened
    opened_at: Option<Instant>,
}

impl CircuitBreaker {
    /// Creates a new circuit breaker.
    ///
    /// # Arguments
    ///
    /// * `failure_threshold` - Failures to trip circuit (e.g., 5)
    /// * `success_threshold` - Successes to close circuit (e.g., 2)
    /// * `timeout` - Wait before retrying after open (e.g., 30s)
    ///
    /// # Example
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use rocketmq_remoting::clients::reconnect::CircuitBreaker;
    ///
    /// let breaker = CircuitBreaker::new(5, 2, Duration::from_secs(30));
    /// ```
    pub fn new(failure_threshold: u32, success_threshold: u32, timeout: Duration) -> Self {
        Self {
            state: CircuitState::Closed,
            failure_count: 0,
            failure_threshold,
            success_threshold,
            success_count: 0,
            timeout,
            opened_at: None,
        }
    }

    /// Creates a default circuit breaker (5 failures, 2 successes, 30s timeout).
    pub fn default_breaker() -> Self {
        Self::new(5, 2, Duration::from_secs(30))
    }

    /// Checks if a request should be allowed.
    ///
    /// # Returns
    ///
    /// * `true` - Request allowed (CLOSED or HALF_OPEN)
    /// * `false` - Request blocked (OPEN and timeout not elapsed)
    pub fn allow_request(&mut self) -> bool {
        match self.state {
            CircuitState::Closed => true,
            CircuitState::HalfOpen => true,
            CircuitState::Open => {
                // Check if timeout elapsed
                if let Some(opened_at) = self.opened_at {
                    if opened_at.elapsed() >= self.timeout {
                        // Transition to HALF_OPEN
                        self.state = CircuitState::HalfOpen;
                        self.success_count = 0;
                        self.failure_count = 0;
                        true
                    } else {
                        false // Still open, reject request
                    }
                } else {
                    false
                }
            }
        }
    }

    /// Records a successful request.
    pub fn record_success(&mut self) {
        match self.state {
            CircuitState::Closed => {
                // Reset failure counter
                self.failure_count = 0;
            }
            CircuitState::HalfOpen => {
                self.success_count += 1;
                if self.success_count >= self.success_threshold {
                    // Close circuit
                    self.state = CircuitState::Closed;
                    self.success_count = 0;
                    self.failure_count = 0;
                    self.opened_at = None;
                }
            }
            CircuitState::Open => {
                // Should not happen (requests blocked in OPEN)
            }
        }
    }

    /// Records a failed request.
    pub fn record_failure(&mut self) {
        match self.state {
            CircuitState::Closed => {
                self.failure_count += 1;
                if self.failure_count >= self.failure_threshold {
                    // Open circuit
                    self.state = CircuitState::Open;
                    self.opened_at = Some(Instant::now());
                }
            }
            CircuitState::HalfOpen => {
                // Any failure in HALF_OPEN reopens circuit
                self.state = CircuitState::Open;
                self.opened_at = Some(Instant::now());
                self.success_count = 0;
            }
            CircuitState::Open => {
                // Already open, update timestamp
                self.opened_at = Some(Instant::now());
            }
        }
    }

    /// Gets the current circuit state.
    pub fn state(&self) -> CircuitState {
        self.state
    }

    /// Manually resets the circuit to CLOSED.
    pub fn reset(&mut self) {
        self.state = CircuitState::Closed;
        self.failure_count = 0;
        self.success_count = 0;
        self.opened_at = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exponential_backoff() {
        let mut backoff = ExponentialBackoff::new(Duration::from_secs(1), Duration::from_secs(60), 5);

        // First few attempts
        assert_eq!(backoff.next_delay(), Some(Duration::from_secs(1)));
        assert_eq!(backoff.next_delay(), Some(Duration::from_secs(2)));
        assert_eq!(backoff.next_delay(), Some(Duration::from_secs(4)));
        assert_eq!(backoff.next_delay(), Some(Duration::from_secs(8)));
        assert_eq!(backoff.next_delay(), Some(Duration::from_secs(16)));

        // Max attempts reached
        assert_eq!(backoff.next_delay(), None);
    }

    #[test]
    fn test_backoff_reset() {
        let mut backoff = ExponentialBackoff::default_strategy();

        backoff.next_delay();
        backoff.next_delay();
        assert_eq!(backoff.current_attempt(), 2);

        backoff.reset();
        assert_eq!(backoff.current_attempt(), 0);
    }

    #[test]
    fn test_circuit_breaker_closed_to_open() {
        let mut breaker = CircuitBreaker::new(3, 2, Duration::from_secs(1));

        assert_eq!(breaker.state(), CircuitState::Closed);
        assert!(breaker.allow_request());

        // Record failures
        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Closed);

        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Closed);

        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);
        assert!(!breaker.allow_request()); // Blocked
    }

    #[test]
    fn test_circuit_breaker_half_open() {
        let mut breaker = CircuitBreaker::new(2, 2, Duration::from_millis(10));

        // Trip circuit
        breaker.record_failure();
        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);

        // Wait for timeout
        std::thread::sleep(Duration::from_millis(15));

        // Should transition to HALF_OPEN
        assert!(breaker.allow_request());
        assert_eq!(breaker.state(), CircuitState::HalfOpen);

        // Record successes to close
        breaker.record_success();
        assert_eq!(breaker.state(), CircuitState::HalfOpen);

        breaker.record_success();
        assert_eq!(breaker.state(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_half_open_failure() {
        let mut breaker = CircuitBreaker::new(2, 2, Duration::from_millis(10));

        // Trip circuit
        breaker.record_failure();
        breaker.record_failure();

        // Wait and transition to HALF_OPEN
        std::thread::sleep(Duration::from_millis(15));
        breaker.allow_request();
        assert_eq!(breaker.state(), CircuitState::HalfOpen);

        // Failure in HALF_OPEN reopens circuit
        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);
    }
}
