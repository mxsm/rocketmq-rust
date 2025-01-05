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

/// A trait that defines a reference resource with various lifecycle methods.
/// Implementors of this trait must be thread-safe (`Send` and `Sync`).
pub trait ReferenceResource: Send + Sync {
    /// Increases the reference count of the resource.
    ///
    /// # Returns
    /// * `true` if the reference count was successfully increased.
    /// * `false` if the resource is not available.
    fn hold(&self) -> bool;

    /// Checks if the resource is available.
    ///
    /// # Returns
    /// * `true` if the resource is available.
    /// * `false` otherwise.
    fn is_available(&self) -> bool;

    /// Shuts down the resource.
    ///
    /// # Parameters
    /// * `interval_forcibly` - The interval in milliseconds to forcibly shut down the resource.
    fn shutdown(&self, interval_forcibly: u64);

    /// Releases the resource, decreasing the reference count.
    fn release(&self);

    /// Gets the current reference count of the resource.
    ///
    /// # Returns
    /// The current reference count.
    fn get_ref_count(&self) -> i64;

    /// Cleans up the resource if the current reference count matches the provided value.
    ///
    /// # Parameters
    /// * `current_ref` - The reference count to match.
    ///
    /// # Returns
    /// * `true` if the resource was successfully cleaned up.
    /// * `false` otherwise.
    fn cleanup(&self, current_ref: i64) -> bool;

    /// Checks if the cleanup process is over.
    ///
    /// # Returns
    /// * `true` if the cleanup is over.
    /// * `false` otherwise.
    fn is_cleanup_over(&self) -> bool;
}
