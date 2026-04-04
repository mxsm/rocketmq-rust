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

use std::collections::HashSet;
use std::str;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::common::filter::expression_type::ExpressionType;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_filter::filter::FilterFactory;
use rocketmq_filter::utils::bloom_filter::BloomFilter;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_store::config::message_store_config::MessageStoreConfig;

use crate::broker_path_config_helper::get_consumer_filter_path;
use crate::filter::consumer_filter_data::ConsumerFilterData;
use crate::filter::manager::consumer_filter_wrapper::ConsumerFilterWrapper;
use crate::filter::manager::consumer_filter_wrapper::FilterDataMapByTopic;

const MS_24_HOUR: u64 = 24 * 60 * 60 * 1000;
const LOAD_DEAD_MARKER_MS: u64 = 30 * 1000;

#[derive(Clone)]
pub(crate) struct ConsumerFilterManager {
    _broker_config: Arc<BrokerConfig>,
    message_store_config: Arc<MessageStoreConfig>,
    consumer_filter_wrapper: Arc<parking_lot::RwLock<ConsumerFilterWrapper>>,
    bloom_filter: Option<BloomFilter>,
}

impl ConsumerFilterManager {
    pub fn new(mut broker_config: Arc<BrokerConfig>, message_store_config: Arc<MessageStoreConfig>) -> Self {
        let consumer_filter_wrapper = Arc::new(parking_lot::RwLock::new(ConsumerFilterWrapper::default()));
        let bloom_filter = BloomFilter::create_by_fn(
            broker_config.max_error_rate_of_bloom_filter,
            broker_config.expect_consumer_num_use_filter,
        )
        .unwrap();
        let broker_config_mut = Arc::make_mut(&mut broker_config);
        broker_config_mut.bit_map_length_consume_queue_ext = bloom_filter.m();
        ConsumerFilterManager {
            _broker_config: broker_config,
            message_store_config,
            consumer_filter_wrapper,
            bloom_filter: Some(bloom_filter),
        }
    }

    pub fn build(
        topic: CheetahString,
        consumer_group: CheetahString,
        expression: Option<CheetahString>,
        type_: Option<CheetahString>,
        client_version: u64,
    ) -> Option<ConsumerFilterData> {
        if ExpressionType::is_tag_type(type_.as_deref()) {
            return None;
        }

        let expression_text = expression.as_ref().filter(|value| !value.is_empty())?;
        let expression_type = type_.as_ref()?;
        let filter = FilterFactory::instance().get(expression_type.as_str())?;
        let compiled = filter.compile(expression_text.as_str()).ok()?;

        let mut consumer_filter_data = ConsumerFilterData::default();
        consumer_filter_data.set_topic(topic);
        consumer_filter_data.set_consumer_group(consumer_group);
        consumer_filter_data.set_born_time(current_millis());
        consumer_filter_data.set_dead_time(0);
        consumer_filter_data.set_expression(expression);
        consumer_filter_data.set_expression_type(type_);
        consumer_filter_data.set_client_version(client_version);
        consumer_filter_data.set_compiled_expression(compiled);

        Some(consumer_filter_data)
    }

    pub fn register(&self, consumer_group: &str, subscriptions: &HashSet<SubscriptionData>) {
        let mut active_topics = HashSet::with_capacity(subscriptions.len());
        for subscription in subscriptions {
            active_topics.insert(subscription.topic.to_string());
            self.register_subscription(consumer_group, subscription);
        }

        let now = current_millis();
        let mut wrapper = self.consumer_filter_wrapper.write();
        for by_topic in wrapper.filter_data_by_topic.values_mut() {
            if let Some(filter_data) = by_topic.filter_data_map.get_mut(consumer_group) {
                if !active_topics.contains(filter_data.topic().as_str()) && !filter_data.is_dead() {
                    filter_data.set_dead_time(now);
                }
            }
        }
    }

    pub fn unregister(&self, consumer_group: &str) {
        let now = current_millis();
        let mut wrapper = self.consumer_filter_wrapper.write();
        for by_topic in wrapper.filter_data_by_topic.values_mut() {
            if let Some(filter_data) = by_topic.filter_data_map.get_mut(consumer_group) {
                if !filter_data.is_dead() {
                    filter_data.set_dead_time(now);
                }
            }
        }
    }

    pub fn get_consumer_filter_data(
        &self,
        topic: &CheetahString,
        consumer_group: &CheetahString,
    ) -> Option<ConsumerFilterData> {
        self.consumer_filter_wrapper
            .read()
            .filter_data_by_topic
            .get(topic.as_str())
            .and_then(|by_topic| by_topic.filter_data_map.get(consumer_group.as_str()))
            .cloned()
    }

    pub fn bloom_filter(&self) -> Option<&BloomFilter> {
        self.bloom_filter.as_ref()
    }

    pub fn get(&self, topic: &CheetahString) -> Option<Vec<ConsumerFilterData>> {
        let wrapper = self.consumer_filter_wrapper.read();
        let by_topic = wrapper.filter_data_by_topic.get(topic.as_str())?;
        if by_topic.filter_data_map.is_empty() {
            None
        } else {
            Some(by_topic.filter_data_map.values().cloned().collect())
        }
    }

    fn register_subscription(&self, consumer_group: &str, subscription: &SubscriptionData) -> bool {
        self.register_entry(
            subscription.topic.as_str(),
            consumer_group,
            subscription.sub_string.as_str(),
            subscription.expression_type.as_str(),
            subscription.sub_version as u64,
        )
    }

    fn register_entry(
        &self,
        topic: &str,
        consumer_group: &str,
        expression: &str,
        type_: &str,
        client_version: u64,
    ) -> bool {
        if ExpressionType::is_tag_type(Some(type_)) || expression.is_empty() {
            return false;
        }

        let bloom_filter_data = match self.bloom_filter.as_ref() {
            Some(filter) => filter.generate(&format!("{consumer_group}#{topic}")),
            None => return false,
        };

        let mut wrapper = self.consumer_filter_wrapper.write();
        let by_topic = wrapper
            .filter_data_by_topic
            .entry(topic.to_string())
            .or_insert_with(|| FilterDataMapByTopic {
                filter_data_map: Default::default(),
                topic: topic.to_string(),
            });

        let existing = by_topic.filter_data_map.get(consumer_group).cloned();
        match existing {
            None => {
                let mut filter_data = match Self::build(
                    CheetahString::from_slice(topic),
                    CheetahString::from_slice(consumer_group),
                    Some(CheetahString::from_slice(expression)),
                    Some(CheetahString::from_slice(type_)),
                    client_version,
                ) {
                    Some(filter_data) => filter_data,
                    None => return false,
                };
                filter_data.set_bloom_filter_data(Some(bloom_filter_data));
                by_topic.filter_data_map.insert(consumer_group.to_string(), filter_data);
                true
            }
            Some(old) if client_version <= old.client_version() => {
                if client_version == old.client_version() && old.is_dead() {
                    if let Some(old) = by_topic.filter_data_map.get_mut(consumer_group) {
                        old.set_dead_time(0);
                    }
                    return true;
                }
                false
            }
            Some(old) => {
                let changed = old.expression().map(|value| value.as_str()) != Some(expression)
                    || old.expression_type().map(|value| value.as_str()) != Some(type_)
                    || old.bloom_filter_data() != Some(&bloom_filter_data);

                if changed {
                    let mut filter_data = match Self::build(
                        CheetahString::from_slice(topic),
                        CheetahString::from_slice(consumer_group),
                        Some(CheetahString::from_slice(expression)),
                        Some(CheetahString::from_slice(type_)),
                        client_version,
                    ) {
                        Some(filter_data) => filter_data,
                        None => {
                            by_topic.filter_data_map.remove(consumer_group);
                            return false;
                        }
                    };
                    filter_data.set_bloom_filter_data(Some(bloom_filter_data));
                    by_topic.filter_data_map.insert(consumer_group.to_string(), filter_data);
                    true
                } else {
                    if let Some(old) = by_topic.filter_data_map.get_mut(consumer_group) {
                        old.set_client_version(client_version);
                        if old.is_dead() {
                            old.set_dead_time(0);
                        }
                    }
                    true
                }
            }
        }
    }

    fn compile_filter_data(&self, filter_data: &mut ConsumerFilterData) -> bool {
        let Some(expression) = filter_data.expression() else {
            return false;
        };
        let Some(expression_type) = filter_data.expression_type() else {
            return false;
        };
        let Some(filter) = FilterFactory::instance().get(expression_type.as_str()) else {
            return false;
        };
        let Ok(compiled) = filter.compile(expression.as_str()) else {
            return false;
        };
        filter_data.set_compiled_expression(compiled);
        true
    }

    fn clean(&self) {
        let mut wrapper = self.consumer_filter_wrapper.write();
        wrapper.filter_data_by_topic.retain(|_, by_topic| {
            by_topic.filter_data_map.retain(|_, filter_data| {
                filter_data
                    .how_long_after_death()
                    .map(|elapsed| elapsed < MS_24_HOUR)
                    .unwrap_or(true)
            });
            !by_topic.filter_data_map.is_empty()
        });
    }
}

impl ConfigManager for ConsumerFilterManager {
    fn decode0(&mut self, _key: &[u8], body: &[u8]) {
        if let Ok(json_string) = str::from_utf8(body) {
            self.decode(json_string);
        }
    }

    fn stop(&mut self) -> bool {
        true
    }

    fn config_file_path(&self) -> String {
        get_consumer_filter_path(self.message_store_config.store_path_root_dir.as_str())
    }

    fn encode_pretty(&self, pretty_format: bool) -> String {
        self.clean();
        let wrapper = self.consumer_filter_wrapper.read().clone();
        if pretty_format {
            wrapper.serialize_json_pretty().unwrap_or_default()
        } else {
            wrapper.serialize_json().unwrap_or_default()
        }
    }

    fn decode(&self, json_string: &str) {
        if json_string.is_empty() {
            return;
        }

        let Ok(mut wrapper) = serde_json::from_str::<ConsumerFilterWrapper>(json_string) else {
            return;
        };

        let mut bloom_changed = false;
        let now = current_millis();
        wrapper.filter_data_by_topic.retain(|_, by_topic| {
            by_topic.filter_data_map.retain(|_, filter_data| {
                if !self.compile_filter_data(filter_data) {
                    return false;
                }

                if !self
                    .bloom_filter
                    .as_ref()
                    .is_some_and(|bloom_filter| bloom_filter.is_valid(filter_data.bloom_filter_data()))
                {
                    bloom_changed = true;
                    return false;
                }

                if filter_data.dead_time() == 0 {
                    let dead_time = now.saturating_sub(LOAD_DEAD_MARKER_MS).max(filter_data.born_time());
                    filter_data.set_dead_time(dead_time);
                }

                true
            });

            !by_topic.filter_data_map.is_empty()
        });

        if !bloom_changed {
            *self.consumer_filter_wrapper.write() = wrapper;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_filter::expression::MessageEvaluationContext;
    use rocketmq_filter::expression::Value;

    fn new_manager() -> ConsumerFilterManager {
        ConsumerFilterManager::new(
            Arc::new(BrokerConfig::default()),
            Arc::new(MessageStoreConfig::default()),
        )
    }

    fn sql_subscription(topic: &str, expression: &str, version: i64) -> SubscriptionData {
        SubscriptionData {
            topic: CheetahString::from_slice(topic),
            sub_string: CheetahString::from_slice(expression),
            expression_type: CheetahString::from_static_str(ExpressionType::SQL92),
            sub_version: version,
            ..Default::default()
        }
    }

    #[test]
    fn build_compiles_sql_expression() {
        let filter_data = ConsumerFilterManager::build(
            CheetahString::from_slice("TopicTest"),
            CheetahString::from_slice("GroupTest"),
            Some(CheetahString::from_slice("color = 'blue'")),
            Some(CheetahString::from_static_str(ExpressionType::SQL92)),
            7,
        )
        .expect("SQL filter should be built");

        let mut context = MessageEvaluationContext::default();
        context.put("color", "blue");

        assert_eq!(
            filter_data
                .compiled_expression()
                .as_ref()
                .unwrap()
                .evaluate(&context)
                .unwrap(),
            Value::Boolean(true)
        );
    }

    #[test]
    fn register_and_get_consumer_filter_data() {
        let manager = new_manager();
        let subscriptions = HashSet::from([sql_subscription("TopicTest", "color = 'blue'", 9)]);

        manager.register("GroupTest", &subscriptions);

        let filter_data = manager
            .get_consumer_filter_data(
                &CheetahString::from_slice("TopicTest"),
                &CheetahString::from_slice("GroupTest"),
            )
            .expect("registered filter data should be stored");

        assert!(filter_data.compiled_expression().is_some());
        assert!(filter_data.bloom_filter_data().is_some());
    }

    #[test]
    fn decode_restores_compiled_expression() {
        let manager = new_manager();
        let subscriptions = HashSet::from([sql_subscription("TopicTest", "color = 'blue'", 9)]);
        manager.register("GroupTest", &subscriptions);

        let encoded = manager.encode_pretty(false);

        let restored = new_manager();
        restored.decode(&encoded);
        let filter_data = restored
            .get_consumer_filter_data(
                &CheetahString::from_slice("TopicTest"),
                &CheetahString::from_slice("GroupTest"),
            )
            .expect("decoded filter data should exist");

        let mut context = MessageEvaluationContext::default();
        context.put("color", "blue");

        assert!(filter_data.dead_time() >= filter_data.born_time());
        assert_eq!(
            filter_data
                .compiled_expression()
                .as_ref()
                .unwrap()
                .evaluate(&context)
                .unwrap(),
            Value::Boolean(true)
        );
    }
}
