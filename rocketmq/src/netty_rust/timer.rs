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
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use crate::netty_rust::timeout::Timeout;
use crate::netty_rust::timer_task::TimerTask;

///convert from netty [Timeout](https://github.com/netty/netty/blob/4.1/common/src/main/java/io/netty/util/Timer.java)
/// A trait representing a timer that can schedule and manage timed tasks.
#[allow(dead_code)]
pub trait Timer: Send + Sync {
    /// Schedules a new timeout task to be executed after the specified delay.
    ///
    /// # Arguments
    ///
    /// * `task` - An `Arc` containing the task to be executed.
    /// * `delay` - The duration to wait before executing the task.
    ///
    /// # Returns
    ///
    /// An `Arc` containing the scheduled timeout.
    fn new_timeout(&self, task: Arc<dyn TimerTask>, delay: Duration) -> Arc<dyn Timeout>;

    /// Stops the timer and cancels all scheduled tasks.
    ///
    /// # Returns
    ///
    /// A `HashSet` containing all the cancelled timeouts.
    fn stop(&self) -> HashSet<Arc<dyn Timeout>>;
}
