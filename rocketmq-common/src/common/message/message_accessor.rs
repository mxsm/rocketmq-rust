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
use std::collections::HashMap;

use crate::common::message::message_single::Message;
use crate::common::message::MessageConst;
use crate::common::message::MessageTrait;

pub struct MessageAccessor;

impl MessageAccessor {
    /// Sets the properties of a message.
    ///
    /// # Arguments
    ///
    /// * `msg` - A mutable reference to a message implementing the `MessageTrait`.
    /// * `properties` - A `HashMap` containing the properties to set.
    #[inline]
    pub fn set_properties<T: MessageTrait>(msg: &mut T, properties: HashMap<String, String>) {
        msg.set_properties(properties);
    }

    /// Puts a property into a message.
    ///
    /// # Arguments
    ///
    /// * `msg` - A mutable reference to a message implementing the `MessageTrait`.
    /// * `name` - The name of the property.
    /// * `value` - The value of the property.
    #[inline]
    pub fn put_property<T: MessageTrait>(msg: &mut T, name: &str, value: &str) {
        msg.put_property(name, value);
    }

    /// Clears a property from a message.
    ///
    /// # Arguments
    ///
    /// * `msg` - A mutable reference to a message implementing the `MessageTrait`.
    /// * `name` - The name of the property to clear.
    #[inline]
    pub fn clear_property<T: MessageTrait>(msg: &mut T, name: &str) {
        msg.clear_property(name);
    }

    /// Sets the transfer flag of a message.
    ///
    /// # Arguments
    ///
    /// * `msg` - A mutable reference to a message implementing the `MessageTrait`.
    /// * `unit` - The transfer flag value.
    #[inline]
    pub fn set_transfer_flag<T: MessageTrait>(msg: &mut T, unit: &str) {
        msg.put_property(MessageConst::PROPERTY_TRANSFER_FLAG, unit);
    }

    /// Gets the transfer flag of a message.
    ///
    /// # Arguments
    ///
    /// * `msg` - A reference to a message implementing the `MessageTrait`.
    ///
    /// # Returns
    ///
    /// * `Option<String>` - The transfer flag value if it exists.
    #[inline]
    pub fn get_transfer_flag<T: MessageTrait>(msg: &T) -> Option<String> {
        msg.get_property(MessageConst::PROPERTY_TRANSFER_FLAG)
    }

    /// Sets the correction flag of a message.
    ///
    /// # Arguments
    ///
    /// * `msg` - A mutable reference to a message implementing the `MessageTrait`.
    /// * `unit` - The correction flag value.
    #[inline]
    pub fn set_correction_flag<T: MessageTrait>(msg: &mut T, unit: &str) {
        msg.put_property(MessageConst::PROPERTY_CORRECTION_FLAG, unit);
    }

    /// Gets the correction flag of a message.
    ///
    /// # Arguments
    ///
    /// * `msg` - A reference to a message implementing the `MessageTrait`.
    ///
    /// # Returns
    ///
    /// * `Option<String>` - The correction flag value if it exists.
    #[inline]
    pub fn get_correction_flag<T: MessageTrait>(msg: &T) -> Option<String> {
        msg.get_property(MessageConst::PROPERTY_CORRECTION_FLAG)
    }

    /// Sets the origin message ID of a message.
    ///
    /// # Arguments
    ///
    /// * `msg` - A mutable reference to a message implementing the `MessageTrait`.
    /// * `origin_message_id` - The origin message ID value.
    #[inline]
    pub fn set_origin_message_id<T: MessageTrait>(msg: &mut T, origin_message_id: &str) {
        msg.put_property(MessageConst::PROPERTY_ORIGIN_MESSAGE_ID, origin_message_id);
    }

    /// Gets the origin message ID of a message.
    ///
    /// # Arguments
    ///
    /// * `msg` - A reference to a message implementing the `MessageTrait`.
    ///
    /// # Returns
    ///
    /// * `Option<String>` - The origin message ID value if it exists.
    #[inline]
    pub fn get_origin_message_id<T: MessageTrait>(msg: &T) -> Option<String> {
        msg.get_property(MessageConst::PROPERTY_ORIGIN_MESSAGE_ID)
    }

    /// Sets the MQ2 flag of a message.
    ///
    /// # Arguments
    ///
    /// * `msg` - A mutable reference to a message implementing the `MessageTrait`.
    /// * `flag` - The MQ2 flag value.
    #[inline]
    pub fn set_mq2_flag<T: MessageTrait>(msg: &mut T, flag: &str) {
        msg.put_property(MessageConst::PROPERTY_MQ2_FLAG, flag);
    }

    /// Gets the MQ2 flag of a message.
    ///
    /// # Arguments
    ///
    /// * `msg` - A reference to a message implementing the `MessageTrait`.
    ///
    /// # Returns
    ///
    /// * `Option<String>` - The MQ2 flag value if it exists.
    #[inline]
    pub fn get_mq2_flag<T: MessageTrait>(msg: &T) -> Option<String> {
        msg.get_property(MessageConst::PROPERTY_MQ2_FLAG)
    }

    /// Sets the reconsume time of a message.
    ///
    /// # Arguments
    ///
    /// * `msg` - A mutable reference to a message implementing the `MessageTrait`.
    /// * `reconsume_times` - The reconsume time value.
    #[inline]
    pub fn set_reconsume_time<T: MessageTrait>(msg: &mut T, reconsume_times: &str) {
        msg.put_property(MessageConst::PROPERTY_RECONSUME_TIME, reconsume_times);
    }

    /// Gets the reconsume time of a message.
    ///
    /// # Arguments
    ///
    /// * `msg` - A reference to a message implementing the `MessageTrait`.
    ///
    /// # Returns
    ///
    /// * `Option<String>` - The reconsume time value if it exists.
    #[inline]
    pub fn get_reconsume_time<T: MessageTrait>(msg: &T) -> Option<String> {
        msg.get_property(MessageConst::PROPERTY_RECONSUME_TIME)
    }

    /// Sets the maximum reconsume times of a message.
    ///
    /// # Arguments
    ///
    /// * `msg` - A mutable reference to a message implementing the `MessageTrait`.
    /// * `max_reconsume_times` - The maximum reconsume times value.
    #[inline]
    pub fn set_max_reconsume_times<T: MessageTrait>(msg: &mut T, max_reconsume_times: &str) {
        msg.put_property(
            MessageConst::PROPERTY_MAX_RECONSUME_TIMES,
            max_reconsume_times,
        );
    }

    /// Gets the maximum reconsume times of a message.
    ///
    /// # Arguments
    ///
    /// * `msg` - A reference to a message implementing the `MessageTrait`.
    ///
    /// # Returns
    ///
    /// * `Option<String>` - The maximum reconsume times value if it exists.
    #[inline]
    pub fn get_max_reconsume_times<T: MessageTrait>(msg: &T) -> Option<String> {
        msg.get_property(MessageConst::PROPERTY_MAX_RECONSUME_TIMES)
    }

    /// Sets the consume start timestamp of a message.
    ///
    /// # Arguments
    ///
    /// * `msg` - A mutable reference to a message implementing the `MessageTrait`.
    /// * `property_consume_start_time_stamp` - The consume start timestamp value.
    #[inline]
    pub fn set_consume_start_time_stamp<T: MessageTrait>(
        msg: &mut T,
        property_consume_start_time_stamp: &str,
    ) {
        msg.put_property(
            MessageConst::PROPERTY_CONSUME_START_TIMESTAMP,
            property_consume_start_time_stamp,
        );
    }

    /// Gets the consume start timestamp of a message.
    ///
    /// # Arguments
    ///
    /// * `msg` - A reference to a message implementing the `MessageTrait`.
    ///
    /// # Returns
    ///
    /// * `Option<String>` - The consume start timestamp value if it exists.
    #[inline]
    pub fn get_consume_start_time_stamp<T: MessageTrait>(msg: &T) -> Option<String> {
        msg.get_property(MessageConst::PROPERTY_CONSUME_START_TIMESTAMP)
    }
}
