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

#[derive(Debug, Default)]
pub struct BrokerStatsManager {}

impl BrokerStatsManager {
    pub const TOPIC_PUT_LATENCY: &'static str = "TOPIC_PUT_LATENCY";
    pub const GROUP_ACK_NUMS: &'static str = "GROUP_ACK_NUMS";
    pub const GROUP_CK_NUMS: &'static str = "GROUP_CK_NUMS";
    pub const DLQ_PUT_NUMS: &'static str = "DLQ_PUT_NUMS";
    pub const BROKER_ACK_NUMS: &'static str = "BROKER_ACK_NUMS";
    pub const BROKER_CK_NUMS: &'static str = "BROKER_CK_NUMS";
    pub const BROKER_GET_NUMS_WITHOUT_SYSTEM_TOPIC: &'static str =
        "BROKER_GET_NUMS_WITHOUT_SYSTEM_TOPIC";
    pub const BROKER_PUT_NUMS_WITHOUT_SYSTEM_TOPIC: &'static str =
        "BROKER_PUT_NUMS_WITHOUT_SYSTEM_TOPIC";
    pub const SNDBCK2DLQ_TIMES: &'static str = "SNDBCK2DLQ_TIMES";

    pub const COMMERCIAL_OWNER: &'static str = "Owner";

    pub const ACCOUNT_OWNER_PARENT: &'static str = "OWNER_PARENT";
    pub const ACCOUNT_OWNER_SELF: &'static str = "OWNER_SELF";

    pub const ACCOUNT_STAT_INVERTAL: u64 = 60 * 1000;
    pub const ACCOUNT_AUTH_TYPE: &'static str = "AUTH_TYPE";

    pub const ACCOUNT_SEND: &'static str = "SEND";
    pub const ACCOUNT_RCV: &'static str = "RCV";
    pub const ACCOUNT_SEND_BACK: &'static str = "SEND_BACK";
    pub const ACCOUNT_SEND_BACK_TO_DLQ: &'static str = "SEND_BACK_TO_DLQ";
    pub const ACCOUNT_AUTH_FAILED: &'static str = "AUTH_FAILED";
    pub const ACCOUNT_SEND_REJ: &'static str = "SEND_REJ";
    pub const ACCOUNT_REV_REJ: &'static str = "RCV_REJ";

    pub const MSG_NUM: &'static str = "MSG_NUM";
    pub const MSG_SIZE: &'static str = "MSG_SIZE";
    pub const SUCCESS_MSG_NUM: &'static str = "SUCCESS_MSG_NUM";
    pub const FAILURE_MSG_NUM: &'static str = "FAILURE_MSG_NUM";
    pub const COMMERCIAL_MSG_NUM: &'static str = "COMMERCIAL_MSG_NUM";
    pub const SUCCESS_REQ_NUM: &'static str = "SUCCESS_REQ_NUM";
    pub const FAILURE_REQ_NUM: &'static str = "FAILURE_REQ_NUM";
    pub const SUCCESS_MSG_SIZE: &'static str = "SUCCESS_MSG_SIZE";
    pub const FAILURE_MSG_SIZE: &'static str = "FAILURE_MSG_SIZE";
    pub const RT: &'static str = "RT";
    pub const INNER_RT: &'static str = "INNER_RT";

    #[deprecated]
    pub const GROUP_GET_FALL_SIZE: &'static str = "GROUP_GET_FALL_SIZE";
    #[deprecated]
    pub const GROUP_GET_FALL_TIME: &'static str = "GROUP_GET_FALL_TIME";
    // Pull Message Latency
    #[deprecated]
    pub const GROUP_GET_LATENCY: &'static str = "GROUP_GET_LATENCY";

    // Consumer Register Time
    pub const CONSUMER_REGISTER_TIME: &'static str = "CONSUMER_REGISTER_TIME";
    // Producer Register Time
    pub const PRODUCER_REGISTER_TIME: &'static str = "PRODUCER_REGISTER_TIME";
    pub const CHANNEL_ACTIVITY: &'static str = "CHANNEL_ACTIVITY";
    pub const CHANNEL_ACTIVITY_CONNECT: &'static str = "CONNECT";
    pub const CHANNEL_ACTIVITY_IDLE: &'static str = "IDLE";
    pub const CHANNEL_ACTIVITY_EXCEPTION: &'static str = "EXCEPTION";
    pub const CHANNEL_ACTIVITY_CLOSE: &'static str = "CLOSE";
}
