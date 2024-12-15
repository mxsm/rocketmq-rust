/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::consumer::pop_result::PopResult;

/// Trait for handling the results of a pop operation.
///
/// This trait defines the methods for handling successful and error results of a pop operation.
#[trait_variant::make(PopCallback: Send)]
pub trait PopCallbackInner {
    /// Called when the pop operation is successful.
    ///
    /// # Arguments
    ///
    /// * `pop_result` - The result of the pop operation.
    async fn on_success(&self, pop_result: PopResult);

    /// Called when the pop operation encounters an error.
    ///
    /// # Arguments
    ///
    /// * `e` - The error encountered during the pop operation.
    fn on_error(&self, e: Box<dyn std::error::Error>);
}

/// Implementation of the `PopCallback` trait for any function that matches the required signature.
///
/// This implementation allows any function that takes a `PopResult` and returns a future to be used
/// as a `PopCallback`.
impl<F, Fut> PopCallback for F
where
    F: Fn(Option<PopResult>, Option<Box<dyn std::error::Error>>) -> Fut + Send + Sync,
    Fut: Future<Output = ()> + Send,
{
    /// Calls the function with the pop result when the pop operation is successful.
    ///
    /// # Arguments
    ///
    /// * `pop_result` - The result of the pop operation.
    async fn on_success(&self, pop_result: PopResult) {
        (*self)(Some(pop_result), None).await;
    }

    /// Does nothing when the pop operation encounters an error.
    ///
    /// # Arguments
    ///
    /// * `e` - The error encountered during the pop operation.
    fn on_error(&self, e: Box<dyn std::error::Error>) {
        (*self)(None, Some(e));
    }
}

/// Type alias for a callback function that handles the result of a pop operation.
///
/// This type alias defines a callback function that takes a `PopResult` and returns a boxed future.
pub type PopCallbackFn = Arc<
    dyn Fn(
            Option<PopResult>,
            Option<Box<dyn std::error::Error>>,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>>
        + Send
        + Sync,
>;

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::Arc;

    use super::*;

    struct MockPopCallback;

    impl PopCallbackInner for MockPopCallback {
        async fn on_success(&self, _pop_result: PopResult) {
            // Mock implementation
        }

        fn on_error(&self, _e: Box<dyn Error>) {
            // Mock implementation
        }
    }

    #[tokio::test]
    async fn pop_callback_on_success_called() {
        let callback = MockPopCallback;
        let pop_result = PopResult {
            // Initialize with appropriate values
            msg_found_list: vec![],
            pop_status: Default::default(),
            pop_time: 0,
            invisible_time: 0,
            rest_num: 0,
        };
        callback.on_success(pop_result).await;
        // Assertions to verify on_success behavior
    }

    #[tokio::test]
    async fn pop_callback_on_error_called() {
        let callback = MockPopCallback;
        let error: Box<dyn Error> =
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, "error"));
        callback.on_error(error);
        // Assertions to verify on_error behavior
    }

    #[tokio::test]
    async fn pop_callback_fn_on_success_called() {
        let callback_fn: Arc<
            dyn Fn(PopResult) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> + Send + Sync,
        > = Arc::new(|_pop_result| {
            Box::pin(async {}) as Pin<Box<dyn Future<Output = ()> + Send + Sync>>
        });

        let pop_result = PopResult {
            // Initialize with appropriate values
            msg_found_list: vec![],
            pop_status: Default::default(),
            pop_time: 0,
            invisible_time: 0,
            rest_num: 0,
        };

        callback_fn(pop_result).await;
        // Assertions to verify callback_fn on_success behavior
    }
}
