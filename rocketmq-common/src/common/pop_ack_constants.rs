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
use crate::common::mix_all;
use crate::common::topic::TopicValidator;

pub struct PopAckConstants;

impl PopAckConstants {
    pub const ACK_TIME_INTERVAL: i64 = 1000;
    pub const SECOND: i64 = 1000;
    pub const LOCK_TIME: i64 = 5000;
    pub const RETRY_QUEUE_NUM: i32 = 1;
    pub const REVIVE_GROUP: &'static str = "CID_RMQ_SYS_REVIVE_GROUP";
    pub const LOCAL_HOST: &'static str = "127.0.0.1";
    pub const REVIVE_TOPIC: &'static str = "rmq_sys_REVIVE_LOG_";
    pub const CK_TAG: &'static str = "ck";
    pub const ACK_TAG: &'static str = "ack";
    pub const BATCH_ACK_TAG: &'static str = "bAck";
    pub const SPLIT: &'static str = "@";

    pub fn build_cluster_revive_topic(cluster_name: &str) -> String {
        format!("{}{}", PopAckConstants::REVIVE_TOPIC, cluster_name)
    }

    pub fn is_start_with_revive_prefix(topic_name: &str) -> bool {
        topic_name.starts_with(PopAckConstants::REVIVE_TOPIC)
    }
}
