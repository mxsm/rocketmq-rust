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
use crate::mqtrace::consume_message_context::ConsumeMessageContext;

/// `ConsumeMessageHook` is a trait that provides hooks for consuming messages.
/// Implementors of this trait provide their own logic for what should happen before and after a
/// message is consumed.
pub trait ConsumeMessageHook: Sync + Send + 'static {
    /// Returns the name of the hook.
    /// This is typically used for logging and debugging purposes.
    fn hook_name(&self) -> &str;

    /// This method is called before a message is consumed.
    /// The `context` parameter provides information about the message that is about to be consumed.
    /// Implementors can use this method to perform setup or configuration tasks before the message
    /// is consumed.
    fn consume_message_before(&self, context: &mut ConsumeMessageContext);

    /// This method is called after a message is consumed.
    /// The `context` parameter provides information about the message that was just consumed.
    /// Implementors can use this method to perform cleanup or logging tasks after the message is
    /// consumed.
    fn consume_message_after(&self, context: &mut ConsumeMessageContext);
}
