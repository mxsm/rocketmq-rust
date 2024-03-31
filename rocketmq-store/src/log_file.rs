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

use rocketmq_common::common::message::{
    message_batch::MessageExtBatch, message_single::MessageExtBrokerInner,
};

use crate::base::message_result::PutMessageResult;

pub mod commit_log;
pub mod mapped_file;

#[allow(async_fn_in_trait)]
pub trait MessageStore {
    /// Load previously stored messages.
    ///
    /// Returns `true` if success; `false` otherwise.
    fn load(&mut self) -> bool;

    /// Launch this message store.
    ///
    /// # Throws
    ///
    /// Throws an `Exception` if there is any error.
    fn start(&mut self) -> Result<(), Box<dyn std::error::Error>>;

    async fn put_message(&self, msg: MessageExtBrokerInner) -> PutMessageResult;
}
