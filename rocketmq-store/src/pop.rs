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

pub mod ack_msg;
pub mod batch_ack_msg;
pub mod pop_check_point;

/// A trait representing an acknowledgment message that can be converted to and from `Any`.
pub trait AckMessage {
    /// Converts the acknowledgment message to a reference of type `Any`.
    ///
    /// # Returns
    ///
    /// A reference to the acknowledgment message as `Any`.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Converts the acknowledgment message to a mutable reference of type `Any`.
    ///
    /// # Returns
    ///
    /// A mutable reference to the acknowledgment message as `Any`.
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}
