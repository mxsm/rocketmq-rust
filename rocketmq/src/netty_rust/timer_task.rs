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

use crate::netty_rust::timeout::Timeout;

///convert from netty [TimerTask](https://github.com/netty/netty/blob/4.1/common/src/main/java/io/netty/util/TimerTask.java)
#[allow(dead_code)]
pub trait TimerTask {
    /// Executes the timer task.
    ///
    /// # Arguments
    ///
    /// * `timeout` - An `Arc` containing a reference to a `Timeout` object.
    fn run(&self, timeout: Arc<dyn Timeout>);
}
