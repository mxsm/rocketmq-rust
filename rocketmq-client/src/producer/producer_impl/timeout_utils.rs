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

//! Timeout handling utilities for async operations.
//!
//! This module provides utilities for handling timeouts in async operations
//! using Tokio's time utilities, providing better ergonomics and error handling
//! than manual timeout tracking.

use std::future::Future;
use std::time::Duration;

use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use tokio::time::timeout;

/// Applies a timeout to a future operation.
///
/// This function wraps a future with a timeout, returning a `TimeoutError` if the
/// operation does not complete within the specified duration.
///
/// # Arguments
///
/// * `duration` - The maximum time to wait for the operation to complete
/// * `future` - The future operation to execute
///
/// # Returns
///
/// * `RocketMQResult<T>` - The result of the operation or a timeout error
///
/// # Example
///
/// ```no_run
/// use rocketmq_error::RocketMQResult;
/// use std::time::Duration;
///
/// async fn send_message() -> RocketMQResult<String> {
///     // Simulate work
///     tokio::time::sleep(Duration::from_millis(100)).await;
///     Ok("Message sent".to_string())
/// }
///
/// # async fn example() -> RocketMQResult<()> {
/// use rocketmq_client_rust::producer::producer_impl::timeout_utils::with_timeout;
/// let result: RocketMQResult<String> = with_timeout(Duration::from_secs(1), send_message()).await;
/// assert!(result.is_ok());
/// # Ok(())
/// # }
/// ```
pub async fn with_timeout<F, T>(duration: Duration, future: F) -> RocketMQResult<T>
where
    F: Future<Output = RocketMQResult<T>>,
{
    timeout(duration, future).await.map_err(|_| RocketMQError::Timeout {
        operation: "async operation",
        timeout_ms: duration.as_millis() as u64,
    })?
}

/// Applies a timeout with a custom operation name for better error messages.
///
/// # Arguments
///
/// * `operation` - Name of the operation for error reporting
/// * `duration` - The maximum time to wait
/// * `future` - The future operation to execute
///
/// # Returns
///
/// * `RocketMQResult<T>` - The result of the operation or a timeout error
pub async fn with_timeout_named<F, T>(operation: &'static str, duration: Duration, future: F) -> RocketMQResult<T>
where
    F: Future<Output = RocketMQResult<T>>,
{
    timeout(duration, future).await.map_err(|_| RocketMQError::Timeout {
        operation,
        timeout_ms: duration.as_millis() as u64,
    })?
}

/// Applies a timeout specified in milliseconds.
///
/// # Arguments
///
/// * `operation` - Name of the operation for error reporting
/// * `timeout_ms` - The timeout in milliseconds
/// * `future` - The future operation to execute
///
/// # Returns
///
/// * `RocketMQResult<T>` - The result of the operation or a timeout error
pub async fn with_timeout_millis<F, T>(operation: &'static str, timeout_ms: u64, future: F) -> RocketMQResult<T>
where
    F: Future<Output = RocketMQResult<T>>,
{
    with_timeout_named(operation, Duration::from_millis(timeout_ms), future).await
}

/// Executes multiple futures concurrently with a shared timeout.
///
/// This is useful for operations like batch sending where you want to limit
/// the total time spent across all operations.
///
/// # Arguments
///
/// * `duration` - The total timeout for all operations
/// * `futures` - Vector of futures to execute
///
/// # Returns
///
/// * `RocketMQResult<Vec<T>>` - Results of all completed operations or timeout error
///
/// # Example
///
/// ```no_run
/// use rocketmq_error::RocketMQResult;
/// use std::time::Duration;
///
/// async fn send_message(id: usize) -> RocketMQResult<usize> {
///     tokio::time::sleep(Duration::from_millis(100)).await;
///     Ok(id)
/// }
///
/// # async fn example() -> RocketMQResult<()> {
/// use rocketmq_client_rust::producer::producer_impl::timeout_utils::with_timeout_all;
/// let futures = vec![send_message(1), send_message(2), send_message(3)];
/// let results: RocketMQResult<Vec<usize>> =
///     with_timeout_all(Duration::from_secs(1), futures).await;
/// assert!(results.is_ok());
/// assert_eq!(results.unwrap().len(), 3);
/// # Ok(())
/// # }
/// ```
pub async fn with_timeout_all<F, T>(duration: Duration, futures: Vec<F>) -> RocketMQResult<Vec<T>>
where
    F: Future<Output = RocketMQResult<T>> + Send + 'static,
    T: Send + 'static,
{
    let joined = async move {
        let mut results = Vec::with_capacity(futures.len());
        for future in futures {
            results.push(future.await?);
        }
        RocketMQResult::Ok(results)
    };

    with_timeout(duration, joined).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_with_timeout_success() {
        async fn quick_operation() -> RocketMQResult<String> {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok("Success".to_string())
        }

        let result = with_timeout(Duration::from_millis(100), quick_operation()).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Success");
    }

    #[tokio::test]
    async fn test_with_timeout_failure() {
        async fn slow_operation() -> RocketMQResult<String> {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok("Success".to_string())
        }

        let result = with_timeout(Duration::from_millis(50), slow_operation()).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            RocketMQError::Timeout { operation, timeout_ms } => {
                assert_eq!(operation, "async operation");
                assert_eq!(timeout_ms, 50);
            }
            _ => panic!("Expected timeout error"),
        }
    }

    #[tokio::test]
    async fn test_with_timeout_named() {
        async fn slow_operation() -> RocketMQResult<String> {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok("Success".to_string())
        }

        let result = with_timeout_named("test_operation", Duration::from_millis(50), slow_operation()).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            RocketMQError::Timeout { operation, timeout_ms } => {
                assert_eq!(operation, "test_operation");
                assert_eq!(timeout_ms, 50);
            }
            _ => panic!("Expected timeout error"),
        }
    }

    #[tokio::test]
    async fn test_with_timeout_millis() {
        async fn quick_operation() -> RocketMQResult<String> {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok("Success".to_string())
        }

        let result = with_timeout_millis("test", 100, quick_operation()).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Success");
    }

    #[tokio::test]
    async fn test_with_timeout_all_success() {
        async fn operation(id: usize) -> RocketMQResult<usize> {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(id)
        }

        let futures = vec![operation(1), operation(2), operation(3)];
        let result = with_timeout_all(Duration::from_millis(100), futures).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_with_timeout_all_timeout() {
        async fn slow_operation(id: usize) -> RocketMQResult<usize> {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(id)
        }

        let futures = vec![slow_operation(1), slow_operation(2), slow_operation(3)];
        let result = with_timeout_all(Duration::from_millis(50), futures).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            RocketMQError::Timeout { .. } => {}
            _ => panic!("Expected timeout error"),
        }
    }

    #[tokio::test]
    async fn test_with_timeout_propagates_error() {
        async fn failing_operation() -> RocketMQResult<String> {
            Err(RocketMQError::Internal("Test error".to_string()))
        }

        let result = with_timeout(Duration::from_millis(100), failing_operation()).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            RocketMQError::Internal(msg) => {
                assert_eq!(msg, "Test error");
            }
            _ => panic!("Expected internal error"),
        }
    }
}
