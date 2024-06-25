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

use std::env;

use once_cell::sync::Lazy;

pub const ROCKETMQ_HOME_ENV: &str = "ROCKETMQ_HOME";
pub const ROCKETMQ_HOME_PROPERTY: &str = "rocketmq.home.dir";
pub const NAMESRV_ADDR_ENV: &str = "NAMESRV_ADDR";
pub const NAMESRV_ADDR_PROPERTY: &str = "rocketmq.rocketmq-namesrv.addr";
pub const MESSAGE_COMPRESS_TYPE: &str = "rocketmq.message.compressType";
pub const MESSAGE_COMPRESS_LEVEL: &str = "rocketmq.message.compressLevel";
pub const DEFAULT_NAMESRV_ADDR_LOOKUP: &str = "jmenv.tbsite.net";
pub const WS_DOMAIN_NAME: &str = "rocketmq.rocketmq-namesrv.domain";
pub const DEFAULT_PRODUCER_GROUP: &str = "DEFAULT_PRODUCER";
pub const DEFAULT_CONSUMER_GROUP: &str = "DEFAULT_CONSUMER";
pub const TOOLS_CONSUMER_GROUP: &str = "TOOLS_CONSUMER";
pub const SCHEDULE_CONSUMER_GROUP: &str = "SCHEDULE_CONSUMER";
pub const FILTERSRV_CONSUMER_GROUP: &str = "FILTERSRV_CONSUMER";
pub const MONITOR_CONSUMER_GROUP: &str = "__MONITOR_CONSUMER";
pub const CLIENT_INNER_PRODUCER_GROUP: &str = "CLIENT_INNER_PRODUCER";
pub const SELF_TEST_PRODUCER_GROUP: &str = "SELF_TEST_P_GROUP";
pub const SELF_TEST_CONSUMER_GROUP: &str = "SELF_TEST_C_GROUP";
pub const ONS_HTTP_PROXY_GROUP: &str = "CID_ONS-HTTP-PROXY";
pub const CID_ONSAPI_PERMISSION_GROUP: &str = "CID_ONSAPI_PERMISSION";
pub const CID_ONSAPI_OWNER_GROUP: &str = "CID_ONSAPI_OWNER";
pub const CID_ONSAPI_PULL_GROUP: &str = "CID_ONSAPI_PULL";
pub const CID_RMQ_SYS_PREFIX: &str = "CID_RMQ_SYS_";
pub const IS_SUPPORT_HEART_BEAT_V2: &str = "IS_SUPPORT_HEART_BEAT_V2";
pub const IS_SUB_CHANGE: &str = "IS_SUB_CHANGE";
pub const DEFAULT_CHARSET: &str = "UTF-8";
pub const MASTER_ID: u64 = 0;
pub const FIRST_SLAVE_ID: u64 = 1;
pub const FIRST_BROKER_CONTROLLER_ID: u64 = 1;
pub const UNIT_PRE_SIZE_FOR_MSG: i32 = 28;
pub const ALL_ACK_IN_SYNC_STATE_SET: i32 = -1;
pub const RETRY_GROUP_TOPIC_PREFIX: &str = "%RETRY%";
pub const DLQ_GROUP_TOPIC_PREFIX: &str = "%DLQ%";
pub const REPLY_TOPIC_POSTFIX: &str = "REPLY_TOPIC";
pub const UNIQUE_MSG_QUERY_FLAG: &str = "_UNIQUE_KEY_QUERY";
pub const DEFAULT_TRACE_REGION_ID: &str = "DefaultRegion";
pub const CONSUME_CONTEXT_TYPE: &str = "ConsumeContextType";
pub const CID_SYS_RMQ_TRANS: &str = "CID_RMQ_SYS_TRANS";
pub const ACL_CONF_TOOLS_FILE: &str = "/conf/tools.yml";
pub const REPLY_MESSAGE_FLAG: &str = "reply";
pub const LMQ_PREFIX: &str = "%LMQ%";
pub const LMQ_QUEUE_ID: u64 = 0;
pub const MULTI_DISPATCH_QUEUE_SPLITTER: &str = ",";
pub const REQ_T: &str = "ReqT";
pub const ROCKETMQ_ZONE_ENV: &str = "ROCKETMQ_ZONE";
pub const ROCKETMQ_ZONE_PROPERTY: &str = "rocketmq.zone";
pub const ROCKETMQ_ZONE_MODE_ENV: &str = "ROCKETMQ_ZONE_MODE";
pub const ROCKETMQ_ZONE_MODE_PROPERTY: &str = "rocketmq.zone.mode";
pub const ZONE_NAME: &str = "__ZONE_NAME";
pub const ZONE_MODE: &str = "__ZONE_MODE";
pub const LOGICAL_QUEUE_MOCK_BROKER_PREFIX: &str = "__syslo__";
pub const METADATA_SCOPE_GLOBAL: &str = "__global__";
pub const LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST: &str = "__syslo__none__";
pub static MULTI_PATH_SPLITTER: Lazy<String> =
    Lazy::new(|| env::var("rocketmq.broker.multiPathSplitter").unwrap_or_else(|_| ",".to_string()));

pub fn is_sys_consumer_group(consumer_group: &str) -> bool {
    consumer_group.starts_with(CID_RMQ_SYS_PREFIX)
}

pub fn is_sys_consumer_group_for_no_cold_read_limit(consumer_group: &str) -> bool {
    if consumer_group == DEFAULT_CONSUMER_GROUP
        || consumer_group == TOOLS_CONSUMER_GROUP
        || consumer_group == SCHEDULE_CONSUMER_GROUP
        || consumer_group == FILTERSRV_CONSUMER_GROUP
        || consumer_group == MONITOR_CONSUMER_GROUP
        || consumer_group == SELF_TEST_CONSUMER_GROUP
        || consumer_group == ONS_HTTP_PROXY_GROUP
        || consumer_group == CID_ONSAPI_PERMISSION_GROUP
        || consumer_group == CID_ONSAPI_OWNER_GROUP
        || consumer_group == CID_ONSAPI_PULL_GROUP
        || consumer_group == CID_SYS_RMQ_TRANS
        || consumer_group.starts_with(CID_RMQ_SYS_PREFIX)
    {
        return true;
    }
    false
}

pub fn get_retry_topic(consumer_group: &str) -> String {
    format!("{}{}", RETRY_GROUP_TOPIC_PREFIX, consumer_group)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifies_sys_consumer_group() {
        assert!(is_sys_consumer_group("CID_RMQ_SYS_SOME_GROUP"));
        assert!(!is_sys_consumer_group("NON_SYS_GROUP"));
    }

    #[test]
    fn identifies_sys_consumer_group_for_no_cold_read_limit() {
        assert!(is_sys_consumer_group_for_no_cold_read_limit(
            "DEFAULT_CONSUMER"
        ));
        assert!(is_sys_consumer_group_for_no_cold_read_limit(
            "TOOLS_CONSUMER"
        ));
        assert!(is_sys_consumer_group_for_no_cold_read_limit(
            "SCHEDULE_CONSUMER"
        ));
        assert!(is_sys_consumer_group_for_no_cold_read_limit(
            "FILTERSRV_CONSUMER"
        ));
        assert!(!is_sys_consumer_group_for_no_cold_read_limit(
            "MONITOR_CONSUMER"
        ));
        assert!(!is_sys_consumer_group_for_no_cold_read_limit(
            "SELF_TEST_CONSUMER"
        ));
        assert!(!is_sys_consumer_group_for_no_cold_read_limit(
            "ONS_HTTP_PROXY_GROUP"
        ));
        assert!(is_sys_consumer_group_for_no_cold_read_limit(
            "CID_ONSAPI_PERMISSION"
        ));
        assert!(is_sys_consumer_group_for_no_cold_read_limit(
            "CID_ONSAPI_OWNER"
        ));
        assert!(is_sys_consumer_group_for_no_cold_read_limit(
            "CID_ONSAPI_PULL"
        ));
        assert!(is_sys_consumer_group_for_no_cold_read_limit(
            "CID_RMQ_SYS_TRANS"
        ));
        assert!(is_sys_consumer_group_for_no_cold_read_limit(
            "CID_RMQ_SYS_SOME_GROUP"
        ));
        assert!(!is_sys_consumer_group_for_no_cold_read_limit(
            "NON_SYS_GROUP"
        ));
    }

    #[test]
    fn generates_retry_topic_for_consumer_group() {
        let consumer_group = "test_group";
        let expected = format!("{}{}", RETRY_GROUP_TOPIC_PREFIX, consumer_group);
        assert_eq!(get_retry_topic(consumer_group), expected);
    }

    #[test]
    fn generates_retry_topic_for_empty_consumer_group() {
        let consumer_group = "";
        let expected = RETRY_GROUP_TOPIC_PREFIX.to_string();
        assert_eq!(get_retry_topic(consumer_group), expected);
    }
}
