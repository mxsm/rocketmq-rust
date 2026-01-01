// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use cheetah_string::CheetahString;
use rocketmq_rust::ArcMut;

use crate::consume_queue::cq_ext_unit::CqExtUnit;
use crate::queue::consume_queue::ConsumeQueueTrait;
use crate::queue::consume_queue_ext::ConsumeQueueExt;
use crate::queue::file_queue_life_cycle::FileQueueLifeCycle;

mod batch_consume_queue;
pub mod build_consume_queue;
pub mod consume_queue;
mod consume_queue_ext;
pub mod consume_queue_store;
mod file_queue_life_cycle;
pub mod local_file_consume_queue_store;
mod queue_offset_operator;
pub mod referred_iterator;
pub mod single_consume_queue;

pub type ArcConsumeQueue = ArcMut<Box<dyn ConsumeQueueTrait>>;
pub type ConsumeQueueTable = parking_lot::Mutex<HashMap<CheetahString, HashMap<i32, ArcConsumeQueue>>>;

pub struct CqUnit {
    pub queue_offset: i64,
    pub size: i32,
    pub pos: i64,
    pub batch_num: i16,
    pub tags_code: i64,
    pub cq_ext_unit: Option<CqExtUnit>,
    pub native_buffer: Vec<u8>,
    pub compacted_offset: i32,
}

impl Default for CqUnit {
    fn default() -> Self {
        CqUnit {
            queue_offset: 0,
            size: 0,
            pos: 0,
            batch_num: 1,
            tags_code: 0,
            cq_ext_unit: None,
            native_buffer: vec![],
            compacted_offset: 0,
        }
    }
}

impl CqUnit {
    pub fn get_valid_tags_code_as_long(&self) -> Option<i64> {
        if !self.is_tags_code_valid() {
            return None;
        }
        Some(self.tags_code)
    }

    pub fn is_tags_code_valid(&self) -> bool {
        !ConsumeQueueExt::is_ext_addr(self.tags_code)
    }
}

pub(crate) mod multi_dispatch_utils {
    use rocketmq_common::common::message::MessageConst;
    use rocketmq_common::common::mix_all;
    use rocketmq_common::common::topic::TopicValidator;

    use crate::base::dispatch_request::DispatchRequest;
    use crate::config::message_store_config::MessageStoreConfig;

    pub fn lmq_queue_key(queue_name: &str) -> String {
        format!("{queue_name}-0")
    }

    pub fn is_need_handle_multi_dispatch(cfg: &MessageStoreConfig, topic: &str) -> bool {
        cfg.enable_multi_dispatch
            && !topic.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX)
            && !topic.starts_with(TopicValidator::SYSTEM_TOPIC_PREFIX)
            && topic != TopicValidator::RMQ_SYS_SCHEDULE_TOPIC
    }

    pub fn check_multi_dispatch_queue(cfg: &MessageStoreConfig, dr: &DispatchRequest) -> bool {
        if !is_need_handle_multi_dispatch(cfg, dr.topic.as_str()) {
            return false;
        }
        let Some(props) = &dr.properties_map else {
            return false;
        };
        if props.is_empty() {
            return false;
        }

        let mdq = props.get(MessageConst::PROPERTY_INNER_MULTI_DISPATCH);
        let mqo = props.get(MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET);

        matches!((mdq, mqo), (Some(a), Some(b)) if !a.is_empty() && !b.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_common::common::message::MessageConst;
    use rocketmq_common::common::topic::TopicValidator;

    use super::*;
    use crate::base::dispatch_request::DispatchRequest;
    use crate::config::message_store_config::MessageStoreConfig;

    #[test]
    fn test_lmq_queue_key() {
        assert_eq!(multi_dispatch_utils::lmq_queue_key("Q"), "Q-0");
    }

    #[test]
    fn test_is_need_handle_multi_dispatch() {
        let cfg = MessageStoreConfig {
            enable_multi_dispatch: true,
            ..Default::default()
        };
        assert!(multi_dispatch_utils::is_need_handle_multi_dispatch(&cfg, "user_topic"));
        assert!(!multi_dispatch_utils::is_need_handle_multi_dispatch(
            &cfg,
            "%RETRY%group"
        ));
        assert!(multi_dispatch_utils::is_need_handle_multi_dispatch(
            &cfg,
            "RMQ_SYS_internal"
        ));
        assert!(!multi_dispatch_utils::is_need_handle_multi_dispatch(
            &cfg,
            TopicValidator::RMQ_SYS_SCHEDULE_TOPIC
        ));
        let cfg_off = MessageStoreConfig {
            enable_multi_dispatch: false,
            ..Default::default()
        };
        assert!(!multi_dispatch_utils::is_need_handle_multi_dispatch(
            &cfg_off,
            "user_topic"
        ));
    }

    #[test]
    fn test_check_multi_dispatch_queue() {
        let cfg = MessageStoreConfig {
            enable_multi_dispatch: true,
            ..Default::default()
        };

        // Missing props
        let dr = DispatchRequest {
            properties_map: None,
            ..Default::default()
        };
        assert!(!multi_dispatch_utils::check_multi_dispatch_queue(&cfg, &dr));

        // Empty props
        let dr = DispatchRequest {
            properties_map: Some(HashMap::new()),
            ..Default::default()
        };
        assert!(!multi_dispatch_utils::check_multi_dispatch_queue(&cfg, &dr));

        // With required props
        let mut props = HashMap::new();
        props.insert(MessageConst::PROPERTY_INNER_MULTI_DISPATCH.into(), "q1,q2".into());
        props.insert(MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET.into(), "1,2".into());
        let dr = DispatchRequest {
            properties_map: Some(props),
            ..Default::default()
        };
        assert!(multi_dispatch_utils::check_multi_dispatch_queue(&cfg, &dr));

        // Blank value fails
        let mut props_blank = HashMap::new();
        props_blank.insert(MessageConst::PROPERTY_INNER_MULTI_DISPATCH.into(), " ".into());
        props_blank.insert(MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET.into(), "2".into());
        let dr = DispatchRequest {
            properties_map: Some(props_blank),
            ..Default::default()
        };
        assert!(multi_dispatch_utils::check_multi_dispatch_queue(&cfg, &dr));
    }
}
