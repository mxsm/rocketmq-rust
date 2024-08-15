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
use crate::common::message::message_single::Message;
use crate::common::message::MessageConst;
use crate::common::message::MessageTrait;

pub struct MessageAccessor;

impl MessageAccessor {
    pub fn clear_property<T: MessageTrait>(msg: &mut T, name: &str) {
        msg.clear_property(name);
    }

    pub fn set_transfer_flag<T: MessageTrait>(msg: &mut T, unit: &str) {
        msg.put_property(MessageConst::PROPERTY_TRANSFER_FLAG, unit);
    }

    pub fn get_transfer_flag<T: MessageTrait>(msg: &T) -> Option<String> {
        msg.get_property(MessageConst::PROPERTY_TRANSFER_FLAG)
    }

    pub fn set_correction_flag<T: MessageTrait>(msg: &mut T, unit: &str) {
        msg.put_property(MessageConst::PROPERTY_CORRECTION_FLAG, unit);
    }

    pub fn get_correction_flag<T: MessageTrait>(msg: &T) -> Option<String> {
        msg.get_property(MessageConst::PROPERTY_CORRECTION_FLAG)
    }

    pub fn set_origin_message_id<T: MessageTrait>(msg: &mut T, origin_message_id: &str) {
        msg.put_property(MessageConst::PROPERTY_ORIGIN_MESSAGE_ID, origin_message_id);
    }

    pub fn get_origin_message_id<T: MessageTrait>(msg: &T) -> Option<String> {
        msg.get_property(MessageConst::PROPERTY_ORIGIN_MESSAGE_ID)
    }

    pub fn set_mq2_flag<T: MessageTrait>(msg: &mut T, flag: &str) {
        msg.put_property(MessageConst::PROPERTY_MQ2_FLAG, flag);
    }

    pub fn get_mq2_flag<T: MessageTrait>(msg: &T) -> Option<String> {
        msg.get_property(MessageConst::PROPERTY_MQ2_FLAG)
    }

    pub fn set_reconsume_time<T: MessageTrait>(msg: &mut T, reconsume_times: &str) {
        msg.put_property(MessageConst::PROPERTY_RECONSUME_TIME, reconsume_times);
    }

    #[inline]
    pub fn get_reconsume_time<T: MessageTrait>(msg: &T) -> Option<String> {
        msg.get_property(MessageConst::PROPERTY_RECONSUME_TIME)
    }

    pub fn set_max_reconsume_times<T: MessageTrait>(msg: &mut T, max_reconsume_times: &str) {
        msg.put_property(
            MessageConst::PROPERTY_MAX_RECONSUME_TIMES,
            max_reconsume_times,
        );
    }

    pub fn get_max_reconsume_times<T: MessageTrait>(msg: &T) -> Option<String> {
        msg.get_property(MessageConst::PROPERTY_MAX_RECONSUME_TIMES)
    }

    pub fn set_consume_start_time_stamp<T: MessageTrait>(
        msg: &mut T,
        property_consume_start_time_stamp: &str,
    ) {
        msg.put_property(
            MessageConst::PROPERTY_CONSUME_START_TIMESTAMP,
            property_consume_start_time_stamp,
        );
    }

    pub fn get_consume_start_time_stamp<T: MessageTrait>(msg: &T) -> Option<String> {
        msg.get_property(MessageConst::PROPERTY_CONSUME_START_TIMESTAMP)
    }
}
