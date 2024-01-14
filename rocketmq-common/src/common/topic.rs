/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License; Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing; software
 * distributed under the License is distributed on an "AS IS" BASIS;
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND; either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

pub struct TopicValidator {}
impl TopicValidator {
    pub const UTO_CREATE_TOPIC_KEY_TOPIC: &'static str = "TBW102";
    pub const RMQ_SYS_SCHEDULE_TOPIC: &'static str = "SCHEDULE_TOPIC_XXXX";
    pub const RMQ_SYS_BENCHMARK_TOPIC: &'static str = "BenchmarkTest";
    pub const RMQ_SYS_TRANS_HALF_TOPIC: &'static str = "RMQ_SYS_TRANS_HALF_TOPIC";
    pub const RMQ_SYS_TRACE_TOPIC: &'static str = "RMQ_SYS_TRACE_TOPIC";
    pub const RMQ_SYS_TRANS_OP_HALF_TOPIC: &'static str = "RMQ_SYS_TRANS_OP_HALF_TOPIC";
    pub const RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC: &'static str = "TRANS_CHECK_MAX_TIME_TOPIC";
    pub const RMQ_SYS_SELF_TEST_TOPIC: &'static str = "SELF_TEST_TOPIC";
    pub const RMQ_SYS_OFFSET_MOVED_EVENT: &'static str = "OFFSET_MOVED_EVENT";
    pub const RMQ_SYS_ROCKSDB_OFFSET_TOPIC: &'static str = "CHECKPOINT_TOPIC";
    pub const SYSTEM_TOPIC_PREFIX: &'static str = "rmq_sys_";
    pub const SYNC_BROKER_MEMBER_GROUP_PREFIX: &'static str = "SYNC_BROKER_MEMBER_";
}
