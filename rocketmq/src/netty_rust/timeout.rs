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
use std::sync::Arc;

use crate::netty_rust::timer_task::TimerTask;
use crate::Timer;

///convert from netty [Timeout](https://github.com/netty/netty/blob/4.1/common/src/main/java/io/netty/util/Timeout.java)
/// A trait representing a timeout that can be managed by a timer.
#[allow(dead_code)]
pub trait Timeout {
    /// Returns the timer associated with this timeout.
    ///
    /// # Returns
    ///
    /// An `Arc` containing the timer.
    fn timer(&self) -> Arc<dyn Timer>;

    /// Returns the task associated with this timeout.
    ///
    /// # Returns
    ///
    /// An `Arc` containing the timer task.
    fn task(&self) -> Arc<dyn TimerTask>;

    /// Checks if the timeout has expired.
    ///
    /// # Returns
    ///
    /// `true` if the timeout has expired, `false` otherwise.
    fn is_expired(&self) -> bool;

    /// Checks if the timeout has been cancelled.
    ///
    /// # Returns
    ///
    /// `true` if the timeout has been cancelled, `false` otherwise.
    fn is_cancelled(&self) -> bool;

    /// Cancels the timeout.
    ///
    /// # Returns
    ///
    /// `true` if the timeout was successfully cancelled, `false` otherwise.
    fn cancel(&self) -> bool;
}
