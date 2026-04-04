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
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::common::filter::expression_type::ExpressionType;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_filter::expression::Expression;
use rocketmq_filter::filter::FilterFactory;
use rocketmq_filter::utils::bloom_filter::BloomFilter;
use rocketmq_filter::utils::bloom_filter_data::BloomFilterData;
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
    compiled_expression_cache: Arc<DashMap<String, Arc<dyn Expression + 'static>>>,
    failed_expression_cache: Arc<DashMap<String, String>>,
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
            compiled_expression_cache: Arc::new(DashMap::new()),
            failed_expression_cache: Arc::new(DashMap::new()),
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

    pub fn resolve(
        &self,
        topic: CheetahString,
        consumer_group: CheetahString,
        expression: Option<CheetahString>,
        type_: Option<CheetahString>,
        client_version: u64,
    ) -> Option<ConsumerFilterData> {
        if ExpressionType::is_tag_type(type_.as_deref()) {
            return None;
        }

        if let Some(existing) = self
            .get_consumer_filter_data(&topic, &consumer_group)
            .filter(|filter_data| !filter_data.is_dead())
            .filter(|filter_data| filter_data.expression() == expression.as_ref())
            .filter(|filter_data| filter_data.expression_type() == type_.as_ref())
            .filter(|filter_data| filter_data.client_version() >= client_version)
        {
            return Some(existing);
        }

        let bloom_filter_data = self.generate_bloom_filter_data(consumer_group.as_str(), topic.as_str())?;
        self.build_with_compiled_expression(
            topic,
            consumer_group,
            expression,
            type_,
            client_version,
            Some(bloom_filter_data),
        )
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

        let Some(bloom_filter_data) = self.generate_bloom_filter_data(consumer_group, topic) else {
            return false;
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
                let filter_data = match self.build_with_compiled_expression(
                    CheetahString::from_slice(topic),
                    CheetahString::from_slice(consumer_group),
                    Some(CheetahString::from_slice(expression)),
                    Some(CheetahString::from_slice(type_)),
                    client_version,
                    Some(bloom_filter_data.clone()),
                ) {
                    Some(filter_data) => filter_data,
                    None => return false,
                };
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
                    let filter_data = match self.build_with_compiled_expression(
                        CheetahString::from_slice(topic),
                        CheetahString::from_slice(consumer_group),
                        Some(CheetahString::from_slice(expression)),
                        Some(CheetahString::from_slice(type_)),
                        client_version,
                        Some(bloom_filter_data.clone()),
                    ) {
                        Some(filter_data) => filter_data,
                        None => {
                            by_topic.filter_data_map.remove(consumer_group);
                            return false;
                        }
                    };
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

    fn generate_bloom_filter_data(&self, consumer_group: &str, topic: &str) -> Option<BloomFilterData> {
        self.bloom_filter
            .as_ref()
            .map(|filter| filter.generate(&format!("{consumer_group}#{topic}")))
    }

    fn cache_key(expression_type: &str, expression: &str) -> String {
        format!("{expression_type}\u{0}{expression}")
    }

    fn clear_compile_caches(&self) {
        self.compiled_expression_cache.clear();
        self.failed_expression_cache.clear();
    }

    #[cfg(test)]
    fn cached_expression_count(&self) -> usize {
        self.compiled_expression_cache.len()
    }

    #[cfg(test)]
    fn cached_compile_failure_count(&self) -> usize {
        self.failed_expression_cache.len()
    }

    fn compile_with_cache(&self, expression_type: &str, expression: &str) -> Option<Arc<dyn Expression + 'static>> {
        let cache_key = Self::cache_key(expression_type, expression);
        if self.failed_expression_cache.contains_key(&cache_key) {
            return None;
        }
        match self.compiled_expression_cache.entry(cache_key) {
            Entry::Occupied(entry) => Some(entry.get().clone()),
            Entry::Vacant(entry) => {
                let Some(filter) = FilterFactory::instance().get(expression_type) else {
                    self.failed_expression_cache
                        .insert(entry.key().clone(), format!("unknown filter type: {expression_type}"));
                    return None;
                };
                let compiled = match filter.compile(expression) {
                    Ok(compiled) => Arc::<dyn Expression + 'static>::from(compiled),
                    Err(error) => {
                        self.failed_expression_cache
                            .insert(entry.key().clone(), error.to_string());
                        return None;
                    }
                };
                self.failed_expression_cache.remove(entry.key());
                entry.insert(compiled.clone());
                Some(compiled)
            }
        }
    }

    fn build_with_compiled_expression(
        &self,
        topic: CheetahString,
        consumer_group: CheetahString,
        expression: Option<CheetahString>,
        type_: Option<CheetahString>,
        client_version: u64,
        bloom_filter_data: Option<BloomFilterData>,
    ) -> Option<ConsumerFilterData> {
        if ExpressionType::is_tag_type(type_.as_deref()) {
            return None;
        }

        let expression_text = expression.as_ref().filter(|value| !value.is_empty())?;
        let expression_type = type_.as_ref()?;
        let compiled = self.compile_with_cache(expression_type.as_str(), expression_text.as_str())?;

        let mut consumer_filter_data = ConsumerFilterData::default();
        consumer_filter_data.set_topic(topic);
        consumer_filter_data.set_consumer_group(consumer_group);
        consumer_filter_data.set_born_time(current_millis());
        consumer_filter_data.set_dead_time(0);
        consumer_filter_data.set_expression(expression);
        consumer_filter_data.set_expression_type(type_);
        consumer_filter_data.set_client_version(client_version);
        consumer_filter_data.set_bloom_filter_data(bloom_filter_data);
        consumer_filter_data.set_compiled_expression_arc(compiled);

        Some(consumer_filter_data)
    }

    fn compile_filter_data(&self, filter_data: &mut ConsumerFilterData) -> bool {
        let Some(expression) = filter_data.expression() else {
            return false;
        };
        let Some(expression_type) = filter_data.expression_type() else {
            return false;
        };
        let Some(compiled) = self.compile_with_cache(expression_type.as_str(), expression.as_str()) else {
            return false;
        };
        filter_data.set_compiled_expression_arc(compiled);
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
        self.clear_compile_caches();
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
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;
    use rocketmq_common::common::config_manager::ConfigManager;
    use rocketmq_filter::expression::EvaluationContext;
    use rocketmq_filter::expression::EvaluationError;
    use rocketmq_filter::expression::Expression;
    use rocketmq_filter::expression::MessageEvaluationContext;
    use rocketmq_filter::expression::Value as ExprValue;
    use rocketmq_filter::expression::Value;
    use rocketmq_filter::filter::Filter;
    use rocketmq_filter::filter::FilterError;

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
            Some(CheetahString::from_slice(
                "region IN ('hz', 'sh') AND name CONTAINS 'rocket' AND score BETWEEN 0 AND 100",
            )),
            Some(CheetahString::from_static_str(ExpressionType::SQL92)),
            7,
        )
        .expect("SQL filter should be built");

        let mut context = MessageEvaluationContext::default();
        context.put("region", "sh");
        context.put("name", "rocketmq-rust");
        context.put("score", "99");

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
    fn resolve_reuses_registered_filter_data_for_matching_subscription() {
        let manager = new_manager();
        let subscriptions = HashSet::from([sql_subscription("TopicTest", "color = 'blue'", 9)]);
        manager.register("GroupTest", &subscriptions);

        let registered = manager
            .get_consumer_filter_data(
                &CheetahString::from_slice("TopicTest"),
                &CheetahString::from_slice("GroupTest"),
            )
            .expect("registered filter data should exist");

        let resolved = manager
            .resolve(
                CheetahString::from_slice("TopicTest"),
                CheetahString::from_slice("GroupTest"),
                Some(CheetahString::from_slice("color = 'blue'")),
                Some(CheetahString::from_static_str(ExpressionType::SQL92)),
                9,
            )
            .expect("resolved filter data should exist");

        assert!(resolved.bloom_filter_data().is_some());
        assert!(std::sync::Arc::ptr_eq(
            registered.compiled_expression().as_ref().unwrap(),
            resolved.compiled_expression().as_ref().unwrap()
        ));
    }

    #[test]
    fn resolve_builds_request_scoped_filter_data_with_bloom_and_cached_expression() {
        let manager = new_manager();

        let first = manager
            .resolve(
                CheetahString::from_slice("TopicTest"),
                CheetahString::from_slice("GroupTest"),
                Some(CheetahString::from_slice("color = 'blue'")),
                Some(CheetahString::from_static_str(ExpressionType::SQL92)),
                9,
            )
            .expect("resolved filter data should exist");
        let second = manager
            .resolve(
                CheetahString::from_slice("TopicTest"),
                CheetahString::from_slice("GroupTest"),
                Some(CheetahString::from_slice("color = 'blue'")),
                Some(CheetahString::from_static_str(ExpressionType::SQL92)),
                10,
            )
            .expect("resolved filter data should exist");

        assert!(manager
            .get_consumer_filter_data(
                &CheetahString::from_slice("TopicTest"),
                &CheetahString::from_slice("GroupTest"),
            )
            .is_none());
        assert!(first.bloom_filter_data().is_some());
        assert!(second.bloom_filter_data().is_some());
        assert!(std::sync::Arc::ptr_eq(
            first.compiled_expression().as_ref().unwrap(),
            second.compiled_expression().as_ref().unwrap()
        ));
    }

    #[derive(Debug)]
    struct FailingFilter {
        filter_type: String,
        compile_count: Arc<AtomicUsize>,
    }

    impl Filter for FailingFilter {
        fn compile(&self, _expr: &str) -> Result<Box<dyn Expression>, FilterError> {
            self.compile_count.fetch_add(1, Ordering::Relaxed);
            Err(FilterError::new("expected test compile failure"))
        }

        fn of_type(&self) -> &str {
            self.filter_type.as_str()
        }
    }

    #[derive(Debug)]
    struct LiteralTrueExpression;

    impl std::fmt::Display for LiteralTrueExpression {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "true")
        }
    }

    impl Expression for LiteralTrueExpression {
        fn evaluate(&self, _context: &dyn EvaluationContext) -> Result<ExprValue, EvaluationError> {
            Ok(ExprValue::Boolean(true))
        }
    }

    #[derive(Debug)]
    struct PassingFilter {
        filter_type: String,
    }

    impl Filter for PassingFilter {
        fn compile(&self, _expr: &str) -> Result<Box<dyn Expression>, FilterError> {
            Ok(Box::new(LiteralTrueExpression))
        }

        fn of_type(&self) -> &str {
            self.filter_type.as_str()
        }
    }

    #[test]
    fn resolve_negative_cache_avoids_recompiling_same_invalid_expression() {
        let filter_type = format!("COUNT_FAIL_{}", current_millis());
        let compile_count = Arc::new(AtomicUsize::new(0));
        rocketmq_filter::filter::FilterFactory::instance().register(Arc::new(FailingFilter {
            filter_type: filter_type.clone(),
            compile_count: compile_count.clone(),
        }));

        let manager = new_manager();
        let first = manager.resolve(
            CheetahString::from_slice("TopicTest"),
            CheetahString::from_slice("GroupTest"),
            Some(CheetahString::from_slice("bad expression")),
            Some(CheetahString::from_string(filter_type.clone())),
            9,
        );
        let second = manager.resolve(
            CheetahString::from_slice("TopicTest"),
            CheetahString::from_slice("GroupTest"),
            Some(CheetahString::from_slice("bad expression")),
            Some(CheetahString::from_string(filter_type.clone())),
            10,
        );

        assert!(first.is_none());
        assert!(second.is_none());
        assert_eq!(compile_count.load(Ordering::Relaxed), 1);
        assert_eq!(manager.cached_compile_failure_count(), 1);

        let _ = rocketmq_filter::filter::FilterFactory::instance().unregister(filter_type.as_str());
    }

    #[test]
    fn stop_clears_compile_caches() {
        let passing_type = format!("COUNT_PASS_{}", current_millis());
        rocketmq_filter::filter::FilterFactory::instance().register(Arc::new(PassingFilter {
            filter_type: passing_type.clone(),
        }));

        let mut manager = new_manager();
        let _ = manager.resolve(
            CheetahString::from_slice("TopicTest"),
            CheetahString::from_slice("GroupTest"),
            Some(CheetahString::from_slice("ok")),
            Some(CheetahString::from_string(passing_type.clone())),
            9,
        );
        let _ = manager.resolve(
            CheetahString::from_slice("TopicTest"),
            CheetahString::from_slice("GroupTest"),
            Some(CheetahString::from_slice("bad")),
            Some(CheetahString::from_string("UNKNOWN_FILTER_TYPE".to_string())),
            10,
        );

        assert_eq!(manager.cached_expression_count(), 1);
        assert_eq!(manager.cached_compile_failure_count(), 1);

        assert!(manager.stop());
        assert_eq!(manager.cached_expression_count(), 0);
        assert_eq!(manager.cached_compile_failure_count(), 0);

        let _ = rocketmq_filter::filter::FilterFactory::instance().unregister(passing_type.as_str());
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
