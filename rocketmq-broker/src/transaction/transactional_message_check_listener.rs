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

use rocketmq_common::common::message::message_ext::MessageExt;

/// Trait defining the listener for transactional message checks.
/// This trait provides a method for resolving discarded messages.
pub trait TransactionalMessageCheckListener {
    /// Resolves a discarded message.
    ///
    /// # Arguments
    ///
    /// * `msgExt` - The external message to be resolved.
    ///
    /// # Returns
    ///
    /// An asynchronous operation that resolves the discarded message.
    async fn resolve_discard_msg(&mut self, msg_ext: MessageExt);
}
