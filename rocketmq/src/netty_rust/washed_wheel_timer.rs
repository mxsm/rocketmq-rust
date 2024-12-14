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
use std::sync::atomic::AtomicI32;
use std::sync::Arc;

use crate::netty_rust::timer_task::TimerTask;

const ST_INIT: i32 = 0;
const ST_CANCELLED: i32 = 1;
const ST_EXPIRED: i32 = 2;

pub struct HashedWheelTimer {}

pub struct HashedWheelTimeout {
    timer: HashedWheelTimer,
    task: Arc<dyn TimerTask>,
    deadline: u64,
    state: AtomicI32,

    // remainingRounds will be calculated and set by Worker.transferTimeoutsToBuckets() before the
    // HashedWheelTimeout will be added to the correct HashedWheelBucket.
    remaining_rounds: i64,

    // This will be used to chain timeouts in HashedWheelTimerBucket via a double-linked-list.
    // As only the workerThread will act on it there is no need for synchronization / volatile.
    next: Option<Arc<HashedWheelTimeout>>,
    prev: Option<Arc<HashedWheelTimeout>>,

    // The bucket to which the timeout was added
    bucket: Option<Arc<HashedWheelBucket>>,
}

impl HashedWheelTimeout {
    pub fn new(timer: HashedWheelTimer, task: Arc<dyn TimerTask>, deadline: u64) -> Self {
        HashedWheelTimeout {
            timer,
            task,
            deadline,
            state: AtomicI32::new(ST_INIT),
            remaining_rounds: 0,
            next: None,
            prev: None,
            bucket: None,
        }
    }
}

pub struct HashedWheelBucket {
    head: Option<Arc<HashedWheelTimeout>>,
    tail: Option<Arc<HashedWheelTimeout>>,
}
