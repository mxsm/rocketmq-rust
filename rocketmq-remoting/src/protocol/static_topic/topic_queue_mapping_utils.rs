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
use rocketmq_common::common::mix_all;

use crate::protocol::static_topic::logic_queue_mapping_item::LogicQueueMappingItem;

pub struct TopicQueueMappingUtils;

impl TopicQueueMappingUtils {
    pub fn find_logic_queue_mapping_item(
        mapping_items: &[LogicQueueMappingItem],
        logic_offset: i64,
        ignore_negative: bool,
    ) -> Option<&LogicQueueMappingItem> {
        if mapping_items.is_empty() {
            return None;
        }
        // Could use binary search to improve performance
        for i in (0..mapping_items.len()).rev() {
            let item = &mapping_items[i];
            if ignore_negative && item.logic_offset < 0 {
                continue;
            }
            if logic_offset >= item.logic_offset {
                return Some(item);
            }
        }
        // If not found, maybe out of range, return the first one
        for item in mapping_items.iter() {
            if ignore_negative && item.logic_offset < 0 {
                continue;
            } else {
                return Some(item);
            }
        }
        None
    }

    pub fn find_next<'a>(
        items: &'a [LogicQueueMappingItem],
        current_item: Option<&'a LogicQueueMappingItem>,
        ignore_negative: bool,
    ) -> Option<&'a LogicQueueMappingItem> {
        if items.is_empty() || current_item.is_none() {
            return None;
        }
        let current_item = current_item.unwrap();
        for i in 0..items.len() {
            let item = &items[i];
            if ignore_negative && item.logic_offset < 0 {
                continue;
            }
            if item.gen == current_item.gen {
                if i < items.len() - 1 {
                    let next_item = &items[i + 1];
                    if ignore_negative && next_item.logic_offset < 0 {
                        return None;
                    } else {
                        return Some(next_item);
                    }
                } else {
                    return None;
                }
            }
        }
        None
    }

    pub fn get_mock_broker_name(scope: &str) -> String {
        assert!(!scope.is_empty(), "Scope cannot be null");

        if scope == mix_all::METADATA_SCOPE_GLOBAL {
            return format!(
                "{}{}",
                mix_all::LOGICAL_QUEUE_MOCK_BROKER_PREFIX,
                &scope[2..]
            );
        } else {
            return format!("{}{}", mix_all::LOGICAL_QUEUE_MOCK_BROKER_PREFIX, scope);
        }
    }
}
