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
use std::sync::Arc;
use std::sync::Weak;

use cheetah_string::CheetahString;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::lite::to_lmq_name;
use rocketmq_common::common::lite::LiteSubscriptionAction;
use rocketmq_common::common::lite::OffsetOption;
use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::body::lite_subscription_ctl_request_body::LiteSubscriptionCtlRequestBody;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_store::base::message_store::MessageStore;
use tracing::warn;

use crate::lite::lite_event_dispatcher::LiteEventDispatcher;
use crate::processor::pop_lite_message_processor::PopLiteMessageProcessor;
use crate::processor::pop_lite_message_processor::PopLiteMessageStoreCapability;
use crate::processor::pop_lite_message_processor::PopLiteOffsetCapability;
use crate::subscription::lite_subscription_registry::LiteSubscriptionRegistry;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupManager;

#[derive(Clone)]
pub(crate) struct LiteSubscriptionCtlPolicy {
    max_lite_subscription_count: usize,
}

impl LiteSubscriptionCtlPolicy {
    pub(crate) fn from_config(broker_config: &BrokerConfig) -> Self {
        Self {
            max_lite_subscription_count: broker_config.max_lite_subscription_count as usize,
        }
    }
}

pub(crate) struct LiteSubscriptionCtlContext<MS: MessageStore> {
    policy: LiteSubscriptionCtlPolicy,
    registry: LiteSubscriptionRegistry,
    event_dispatcher: LiteEventDispatcher,
    subscription_group_manager: SubscriptionGroupManager,
    consumer_offset: PopLiteOffsetCapability<MS>,
    message_store: PopLiteMessageStoreCapability<MS>,
    pop_lite_message_processor: Weak<PopLiteMessageProcessor<MS>>,
}

impl<MS: MessageStore> LiteSubscriptionCtlContext<MS> {
    #[allow(
        clippy::too_many_arguments,
        reason = "composition root lists each Lite subscription capability explicitly"
    )]
    pub(crate) fn new(
        policy: LiteSubscriptionCtlPolicy,
        registry: LiteSubscriptionRegistry,
        event_dispatcher: LiteEventDispatcher,
        subscription_group_manager: SubscriptionGroupManager,
        consumer_offset: PopLiteOffsetCapability<MS>,
        message_store: PopLiteMessageStoreCapability<MS>,
        pop_lite_message_processor: Weak<PopLiteMessageProcessor<MS>>,
    ) -> Self {
        Self {
            policy,
            registry,
            event_dispatcher,
            subscription_group_manager,
            consumer_offset,
            message_store,
            pop_lite_message_processor,
        }
    }
}

pub(crate) struct LiteSubscriptionCtlProcessor<MS: MessageStore> {
    context: LiteSubscriptionCtlContext<MS>,
}

impl<MS: MessageStore> LiteSubscriptionCtlProcessor<MS> {
    pub(crate) fn new(context: LiteSubscriptionCtlContext<MS>) -> Self {
        Self { context }
    }
}

impl<MS: MessageStore> LiteSubscriptionCtlProcessor<MS> {
    pub(crate) async fn process_request_shared(
        &self,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let body = match request.body() {
            Some(body) if !body.is_empty() => body,
            _ => {
                return Ok(Some(self.response_with_code(
                    request,
                    ResponseCode::IllegalOperation,
                    "Request body is null.",
                )));
            }
        };

        let request_body = match SerdeJsonUtils::from_json_bytes::<LiteSubscriptionCtlRequestBody>(body.as_ref()) {
            Ok(body) => body,
            Err(error) => {
                return Ok(Some(self.response_with_code(
                    request,
                    ResponseCode::IllegalOperation,
                    format!("Failed to decode LiteSubscriptionCtlRequestBody: {error}"),
                )));
            }
        };

        if request_body.subscription_set().is_empty() {
            return Ok(Some(self.response_with_code(
                request,
                ResponseCode::IllegalOperation,
                "LiteSubscriptionCtlRequestBody is empty.",
            )));
        }

        for entry in request_body.subscription_set() {
            if entry.client_id().is_empty() {
                warn!("clientId is blank, {}", entry);
                continue;
            }
            if entry.group().is_empty() {
                warn!("group is blank, {}", entry);
                continue;
            }
            if entry.topic().is_empty() {
                warn!("topic is blank, {}", entry);
                continue;
            }

            let registry = &self.context.registry;
            let lmq_name_set = self.to_lmq_name_set(entry.topic(), entry.lite_topic_set());

            match entry.action() {
                LiteSubscriptionAction::PartialAdd => {
                    let group_config = match self.validate_consumer_group(entry.group(), entry.topic()) {
                        Ok(group_config) => group_config,
                        Err((code, remark)) => {
                            return Ok(Some(self.response_with_code(request, code, remark)));
                        }
                    };
                    if let Err((code, remark)) =
                        self.ensure_quota(entry.client_id(), entry.group(), entry.topic(), &lmq_name_set)
                    {
                        return Ok(Some(self.response_with_code(request, code, remark)));
                    }
                    registry.update_client_channel(entry.client_id(), channel.clone());
                    self.context.event_dispatcher.touch_client(entry.client_id());
                    let removed_lmq_names = if group_config.lite_sub_exclusive() {
                        self.exclude_conflicting_subscriptions(
                            entry.client_id(),
                            entry.group(),
                            entry.topic(),
                            &lmq_name_set,
                        )
                    } else {
                        HashSet::new()
                    };
                    registry.add_partial_subscription(entry.client_id(), entry.group(), entry.topic(), &lmq_name_set);
                    if group_config.reset_offset_in_exclusive_mode() {
                        for lmq_name in &removed_lmq_names {
                            self.reset_offset(
                                entry.group(),
                                lmq_name,
                                Some(OffsetOption::policy(OffsetOption::POLICY_MIN_VALUE)),
                            );
                        }
                    }
                    for lmq_name in &lmq_name_set {
                        self.reset_offset(entry.group(), lmq_name, entry.offset_option());
                    }
                }
                LiteSubscriptionAction::PartialRemove => {
                    let removed_lmq_names = registry
                        .lite_subscription(entry.client_id(), entry.group(), entry.topic())
                        .map(|subscription| {
                            subscription
                                .lite_topic_set()
                                .intersection(&lmq_name_set)
                                .cloned()
                                .collect::<HashSet<_>>()
                        })
                        .unwrap_or_default();
                    registry.remove_partial_subscription(
                        entry.client_id(),
                        entry.group(),
                        entry.topic(),
                        &lmq_name_set,
                    );
                    if self.should_reset_offset_on_unsubscribe(entry.group()) {
                        for lmq_name in &removed_lmq_names {
                            self.reset_offset(
                                entry.group(),
                                lmq_name,
                                Some(OffsetOption::policy(OffsetOption::POLICY_MIN_VALUE)),
                            );
                        }
                    }
                }
                LiteSubscriptionAction::CompleteAdd => {
                    if let Err((code, remark)) = self.validate_consumer_group(entry.group(), entry.topic()) {
                        return Ok(Some(self.response_with_code(request, code, remark)));
                    }
                    registry.update_client_channel(entry.client_id(), channel.clone());
                    self.context.event_dispatcher.touch_client(entry.client_id());
                    registry.add_complete_subscription(
                        entry.client_id(),
                        entry.group(),
                        entry.topic(),
                        &lmq_name_set,
                        entry.version(),
                    );
                }
                LiteSubscriptionAction::CompleteRemove => {
                    registry.remove_complete_subscription(entry.client_id());
                }
            }
        }

        Ok(Some(
            RemotingCommand::create_response_command().set_opaque(request.opaque()),
        ))
    }
}

impl<MS: MessageStore> RequestProcessor for LiteSubscriptionCtlProcessor<MS> {
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        self.process_request_shared(channel, ctx, request).await
    }
}

impl<MS: MessageStore> LiteSubscriptionCtlProcessor<MS> {
    fn ensure_quota(
        &self,
        client_id: &CheetahString,
        group: &CheetahString,
        topic: &CheetahString,
        lmq_name_set: &HashSet<CheetahString>,
    ) -> Result<(), (ResponseCode, CheetahString)> {
        let existing_topics = self
            .context
            .registry
            .lite_subscription(client_id, group, topic)
            .map(|subscription| subscription.lite_topic_set().clone())
            .unwrap_or_default();
        let additional_refs = lmq_name_set.difference(&existing_topics).count();
        if additional_refs == 0 {
            return Ok(());
        }

        let max_count = self.context.policy.max_lite_subscription_count;
        let projected_count = self
            .context
            .registry
            .active_subscription_num()
            .saturating_add(additional_refs);
        if projected_count > max_count {
            return Err((
                ResponseCode::LiteSubscriptionQuotaExceeded,
                CheetahString::from_string(format!("lite subscription quota exceeded {max_count}")),
            ));
        }

        Ok(())
    }

    fn validate_consumer_group(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
    ) -> Result<Arc<SubscriptionGroupConfig>, (ResponseCode, CheetahString)> {
        let group_config = self
            .context
            .subscription_group_manager
            .subscription_group_table()
            .get(group)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or_else(|| {
                (
                    ResponseCode::SubscriptionGroupNotExist,
                    CheetahString::from_string(format!("Group [{}] not exist.", group)),
                )
            })?;

        if !group_config.consume_enable() {
            return Err((
                ResponseCode::IllegalOperation,
                CheetahString::from_static_str("Consumer group is not allowed to consume."),
            ));
        }

        match group_config.lite_bind_topic() {
            Some(bind_topic) if bind_topic == topic => Ok(group_config),
            _ => Err((
                ResponseCode::InvalidParameter,
                CheetahString::from_string(format!("Subscription [{}]-[{}] not match.", group, topic)),
            )),
        }
    }

    fn find_consumer_group_config(&self, group: &CheetahString) -> Option<Arc<SubscriptionGroupConfig>> {
        self.context
            .subscription_group_manager
            .subscription_group_table()
            .get(group)
            .map(|entry| Arc::clone(entry.value()))
    }

    fn should_reset_offset_on_unsubscribe(&self, group: &CheetahString) -> bool {
        self.find_consumer_group_config(group)
            .is_some_and(|group_config| group_config.reset_offset_on_unsubscribe())
    }

    fn exclude_conflicting_subscriptions(
        &self,
        client_id: &CheetahString,
        group: &CheetahString,
        topic: &CheetahString,
        lmq_name_set: &HashSet<CheetahString>,
    ) -> HashSet<CheetahString> {
        let registry = &self.context.registry;
        let conflicts = registry
            .all_subscriptions()
            .into_iter()
            .filter(|subscription| {
                subscription.client_id != *client_id && subscription.group == *group && subscription.topic == *topic
            })
            .map(|subscription| {
                let overlapping = subscription
                    .lite_topic_set
                    .intersection(lmq_name_set)
                    .cloned()
                    .collect::<HashSet<_>>();
                (subscription.client_id, overlapping)
            })
            .filter(|(_, overlapping)| !overlapping.is_empty())
            .collect::<Vec<_>>();

        let mut removed_lmq_names = HashSet::new();
        for (conflict_client_id, overlapping) in conflicts {
            registry.remove_partial_subscription(&conflict_client_id, group, topic, &overlapping);
            removed_lmq_names.extend(overlapping);
        }
        removed_lmq_names
    }

    fn to_lmq_name_set(
        &self,
        topic: &CheetahString,
        lite_topic_set: &HashSet<CheetahString>,
    ) -> HashSet<CheetahString> {
        lite_topic_set
            .iter()
            .filter_map(|lite_topic| to_lmq_name(topic.as_str(), lite_topic.as_str()).map(CheetahString::from_string))
            .collect()
    }

    fn reset_offset(&self, group: &CheetahString, lmq_name: &CheetahString, offset_option: Option<OffsetOption>) {
        let Some(offset_option) = offset_option else {
            return;
        };

        let current_offset = self.context.consumer_offset.query_offset(group, lmq_name);
        let target_offset = match offset_option.type_() {
            rocketmq_common::common::lite::OffsetOptionType::Policy => match offset_option.value() {
                value if value == OffsetOption::POLICY_MIN_VALUE => Some(0),
                value if value == OffsetOption::POLICY_MAX_VALUE => {
                    Some(self.context.message_store.max_offset(lmq_name))
                }
                _ => None,
            },
            rocketmq_common::common::lite::OffsetOptionType::Offset => Some(offset_option.value()),
            rocketmq_common::common::lite::OffsetOptionType::TailN => {
                (current_offset >= 0).then_some((current_offset - offset_option.value()).max(0))
            }
            rocketmq_common::common::lite::OffsetOptionType::Timestamp => None,
            _ => None,
        };

        if let Some(target_offset) = target_offset {
            if current_offset != target_offset {
                self.context
                    .consumer_offset
                    .assign_reset_offset(lmq_name, group, target_offset);
                if let Some(processor) = self.context.pop_lite_message_processor.upgrade() {
                    processor.clear_order_info(lmq_name, group);
                }
            }
        }
    }

    fn response_with_code(
        &self,
        request: &RemotingCommand,
        code: ResponseCode,
        remark: impl Into<CheetahString>,
    ) -> RemotingCommand {
        RemotingCommand::create_response_command_with_code_remark(code, remark).set_opaque(request.opaque())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::SystemTime;

    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use rocketmq_common::common::attribute::subscription_group_attributes::LITE_BIND_TOPIC_ATTRIBUTE_NAME;
    use rocketmq_common::common::attribute::subscription_group_attributes::LITE_SUB_MODEL_ATTRIBUTE_NAME;
    use rocketmq_common::common::attribute::subscription_group_attributes::LITE_SUB_RESET_OFFSET_EXCLUSIVE_ATTRIBUTE_NAME;
    use rocketmq_common::common::attribute::subscription_group_attributes::LITE_SUB_RESET_OFFSET_UNSUBSCRIBE_ATTRIBUTE_NAME;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::lite::to_lmq_name;
    use rocketmq_common::common::lite::LiteSubscriptionAction;
    use rocketmq_common::common::lite::LiteSubscriptionDTO;
    use rocketmq_common::common::lite::OffsetOption;
    use rocketmq_remoting::base::response_future::ResponseFuture;
    use rocketmq_remoting::code::request_code::RequestCode;
    use rocketmq_remoting::code::response_code::ResponseCode;
    use rocketmq_remoting::connection::Connection;
    use rocketmq_remoting::net::channel::Channel;
    use rocketmq_remoting::net::channel::ChannelInner;
    use rocketmq_remoting::protocol::body::lite_subscription_ctl_request_body::LiteSubscriptionCtlRequestBody;
    use rocketmq_remoting::protocol::header::empty_header::EmptyHeader;
    use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
    use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
    use rocketmq_remoting::runtime::processor::RequestProcessor;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;
    use rocketmq_store::message_store::GenericMessageStore;

    use super::LiteSubscriptionCtlContext;
    use super::LiteSubscriptionCtlPolicy;
    use super::LiteSubscriptionCtlProcessor;
    use crate::broker_runtime::BrokerRuntime;
    use crate::processor::pop_lite_message_processor::PopLiteMessageStoreCapability;
    use crate::processor::pop_lite_message_processor::PopLiteOffsetCapability;

    fn temp_test_root(label: &str) -> std::path::PathBuf {
        let millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("time should move forward")
            .as_millis();
        std::env::temp_dir().join(format!("rocketmq-rust-lite-subscription-{label}-{millis}"))
    }

    async fn new_test_runtime(label: &str) -> BrokerRuntime {
        new_test_runtime_with_max_subscription_count(label, BrokerConfig::default().max_lite_subscription_count).await
    }

    async fn new_test_runtime_with_max_subscription_count(
        label: &str,
        max_lite_subscription_count: u64,
    ) -> BrokerRuntime {
        let temp_root = temp_test_root(label);
        let broker_config = std::sync::Arc::new(BrokerConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            auth_config_path: temp_root.join("auth.json").to_string_lossy().into_owned().into(),
            max_lite_subscription_count,
            ..BrokerConfig::default()
        });
        let message_store_config = std::sync::Arc::new(MessageStoreConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            ..MessageStoreConfig::default()
        });
        let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
        assert!(runtime.initialize().await);
        runtime
    }

    fn lite_subscription_processor_for_test(
        runtime: &mut BrokerRuntime,
    ) -> LiteSubscriptionCtlProcessor<GenericMessageStore> {
        let inner = runtime.inner_for_test();
        let consumer_offset_manager = inner.consumer_offset_manager_handle();
        let escape_bridge = inner.escape_bridge();
        let pop_lite_message_processor = inner
            .pop_lite_message_processor()
            .map(Arc::downgrade)
            .unwrap_or_default();
        LiteSubscriptionCtlProcessor::new(LiteSubscriptionCtlContext::new(
            LiteSubscriptionCtlPolicy::from_config(inner.broker_config()),
            inner.lite_subscription_registry().clone(),
            inner.lite_event_dispatcher().clone(),
            inner.subscription_group_manager().clone(),
            PopLiteOffsetCapability::new(&consumer_offset_manager),
            PopLiteMessageStoreCapability::new(&escape_bridge),
            pop_lite_message_processor,
        ))
    }

    async fn create_test_channel() -> Channel {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind local test listener");
        let local_addr = listener.local_addr().expect("local listener addr");
        let std_stream = std::net::TcpStream::connect(local_addr).expect("connect local test listener");
        std_stream.set_nonblocking(true).expect("set nonblocking");
        drop(listener);
        let tcp_stream = tokio::net::TcpStream::from_std(std_stream).expect("convert tcp stream");
        let connection = Connection::new(tcp_stream);
        let response_table = std::sync::Arc::new(parking_lot::Mutex::new(HashMap::<i32, ResponseFuture>::new()));
        let inner = std::sync::Arc::new(ChannelInner::new(connection, response_table));
        Channel::new(inner, local_addr, local_addr)
    }

    fn seed_group_config(runtime: &mut BrokerRuntime, group: &str, attributes: HashMap<CheetahString, CheetahString>) {
        let mut config =
            rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig::new(
                CheetahString::from(group),
            );
        config.set_attributes(attributes);
        runtime
            .inner_for_test()
            .subscription_group_manager_mut()
            .update_subscription_group_config(&mut config);
    }

    #[test]
    fn lite_subscription_ctl_policy_captures_only_required_startup_value() {
        let broker_config = BrokerConfig {
            max_lite_subscription_count: 37,
            ..Default::default()
        };

        let policy = LiteSubscriptionCtlPolicy::from_config(&broker_config);

        assert_eq!(policy.max_lite_subscription_count, 37);
    }

    #[test]
    fn lite_subscription_ctl_source_uses_only_explicit_capabilities() {
        let source = include_str!("lite_subscription_ctl_processor.rs");
        let production = source
            .split("#[cfg(test)]")
            .next()
            .expect("production source should precede tests");

        assert!(!production.contains(concat!("rocketmq_rust::", "ArcMut")));
        assert!(!production.contains(concat!("BrokerRuntime", "Inner")));
        assert!(!production.contains(concat!("broker_runtime", "_inner")));
        assert!(production.contains("context: LiteSubscriptionCtlContext<MS>"));
        assert!(production.contains("consumer_offset: PopLiteOffsetCapability<MS>"));
        assert!(production.contains("message_store: PopLiteMessageStoreCapability<MS>"));
    }

    #[test]
    fn lite_subscription_ctl_weak_providers_do_not_keep_runtime_or_store_alive() {
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
        let processor = lite_subscription_processor_for_test(&mut runtime);
        let group = CheetahString::from_static_str("group");
        let lmq_name = CheetahString::from_static_str("%LMQ%topic%child");

        drop(runtime);

        assert_eq!(processor.context.consumer_offset.query_offset(&group, &lmq_name), -1);
        assert_eq!(processor.context.message_store.max_offset(&lmq_name), 0);
        assert!(processor.context.pop_lite_message_processor.upgrade().is_none());
    }

    #[tokio::test]
    async fn complete_add_updates_registry_with_lmq_names() {
        let mut runtime = new_test_runtime("processor-success").await;
        let inner = runtime.inner_for_test().clone();
        seed_group_config(
            &mut runtime,
            "lite-group",
            HashMap::from([(
                CheetahString::from_string(format!("+{LITE_BIND_TOPIC_ATTRIBUTE_NAME}")),
                CheetahString::from_static_str("parent-topic"),
            )]),
        );

        let dto = LiteSubscriptionDTO::new()
            .with_action(LiteSubscriptionAction::CompleteAdd)
            .with_client_id(CheetahString::from_static_str("client-id"))
            .with_group(CheetahString::from_static_str("lite-group"))
            .with_topic(CheetahString::from_static_str("parent-topic"))
            .with_lite_topic_set(HashSet::from([CheetahString::from_static_str("child-a")]));
        let mut body = LiteSubscriptionCtlRequestBody::new();
        body.set_subscription_set(vec![dto]);
        let mut request = RemotingCommand::create_request_command(RequestCode::LiteSubscriptionCtl, EmptyHeader {})
            .set_body(Bytes::from(serde_json::to_vec(&body).expect("serialize request body")));

        let channel = create_test_channel().await;
        let ctx = std::sync::Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let mut processor = lite_subscription_processor_for_test(&mut runtime);
        let response = processor
            .process_request(channel, ctx, &mut request)
            .await
            .expect("processor request should succeed")
            .expect("processor should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let subscription = inner
            .lite_subscription_registry()
            .lite_subscription(
                &CheetahString::from_static_str("client-id"),
                &CheetahString::from_static_str("lite-group"),
                &CheetahString::from_static_str("parent-topic"),
            )
            .expect("registry should contain lite subscription");
        assert!(subscription.lite_topic_set().contains(&CheetahString::from_string(
            to_lmq_name("parent-topic", "child-a").expect("convert lite topic")
        )));

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn complete_add_ignores_stale_version_snapshot() {
        let mut runtime = new_test_runtime("processor-stale-version").await;
        let inner = runtime.inner_for_test().clone();
        seed_group_config(
            &mut runtime,
            "lite-group",
            HashMap::from([(
                CheetahString::from_string(format!("+{LITE_BIND_TOPIC_ATTRIBUTE_NAME}")),
                CheetahString::from_static_str("parent-topic"),
            )]),
        );

        let mut body = LiteSubscriptionCtlRequestBody::new();
        body.set_subscription_set(vec![LiteSubscriptionDTO::new()
            .with_action(LiteSubscriptionAction::CompleteAdd)
            .with_client_id(CheetahString::from_static_str("client-id"))
            .with_group(CheetahString::from_static_str("lite-group"))
            .with_topic(CheetahString::from_static_str("parent-topic"))
            .with_lite_topic_set(HashSet::from([
                CheetahString::from_static_str("child-a"),
                CheetahString::from_static_str("child-b"),
            ]))
            .with_version(5)]);
        let mut request = RemotingCommand::create_request_command(RequestCode::LiteSubscriptionCtl, EmptyHeader {})
            .set_body(Bytes::from(serde_json::to_vec(&body).expect("serialize request body")));

        let channel = create_test_channel().await;
        let ctx = std::sync::Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let mut processor = lite_subscription_processor_for_test(&mut runtime);
        let response = processor
            .process_request(channel.clone(), ctx.clone(), &mut request)
            .await
            .expect("processor request should succeed")
            .expect("processor should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);

        let mut stale_body = LiteSubscriptionCtlRequestBody::new();
        stale_body.set_subscription_set(vec![LiteSubscriptionDTO::new()
            .with_action(LiteSubscriptionAction::CompleteAdd)
            .with_client_id(CheetahString::from_static_str("client-id"))
            .with_group(CheetahString::from_static_str("lite-group"))
            .with_topic(CheetahString::from_static_str("parent-topic"))
            .with_lite_topic_set(HashSet::from([CheetahString::from_static_str("child-c")]))
            .with_version(4)]);
        let mut stale_request =
            RemotingCommand::create_request_command(RequestCode::LiteSubscriptionCtl, EmptyHeader {}).set_body(
                Bytes::from(serde_json::to_vec(&stale_body).expect("serialize stale request body")),
            );

        let stale_response = processor
            .process_request(channel, ctx, &mut stale_request)
            .await
            .expect("processor stale request should succeed")
            .expect("processor should return a response");

        assert_eq!(ResponseCode::from(stale_response.code()), ResponseCode::Success);
        let subscription = inner
            .lite_subscription_registry()
            .lite_subscription(
                &CheetahString::from_static_str("client-id"),
                &CheetahString::from_static_str("lite-group"),
                &CheetahString::from_static_str("parent-topic"),
            )
            .expect("registry should retain the latest complete snapshot");
        assert_eq!(subscription.version(), 5);
        assert_eq!(
            subscription.lite_topic_set(),
            &HashSet::from([
                CheetahString::from_string(to_lmq_name("parent-topic", "child-a").expect("convert child-a")),
                CheetahString::from_string(to_lmq_name("parent-topic", "child-b").expect("convert child-b")),
            ])
        );

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn partial_add_returns_quota_exceeded_when_global_limit_is_hit() {
        let mut runtime = new_test_runtime_with_max_subscription_count("quota-exceeded", 1).await;
        let inner = runtime.inner_for_test().clone();
        seed_group_config(
            &mut runtime,
            "lite-group",
            HashMap::from([(
                CheetahString::from_string(format!("+{LITE_BIND_TOPIC_ATTRIBUTE_NAME}")),
                CheetahString::from_static_str("parent-topic"),
            )]),
        );
        inner.lite_subscription_registry().add_partial_subscription(
            &CheetahString::from_static_str("client-1"),
            &CheetahString::from_static_str("lite-group"),
            &CheetahString::from_static_str("parent-topic"),
            &HashSet::from([CheetahString::from_string(
                to_lmq_name("parent-topic", "child-a").expect("convert lite topic"),
            )]),
        );

        let dto = LiteSubscriptionDTO::new()
            .with_action(LiteSubscriptionAction::PartialAdd)
            .with_client_id(CheetahString::from_static_str("client-2"))
            .with_group(CheetahString::from_static_str("lite-group"))
            .with_topic(CheetahString::from_static_str("parent-topic"))
            .with_lite_topic_set(HashSet::from([CheetahString::from_static_str("child-b")]));
        let mut body = LiteSubscriptionCtlRequestBody::new();
        body.set_subscription_set(vec![dto]);
        let mut request = RemotingCommand::create_request_command(RequestCode::LiteSubscriptionCtl, EmptyHeader {})
            .set_body(Bytes::from(serde_json::to_vec(&body).expect("serialize request body")));

        let channel = create_test_channel().await;
        let ctx = std::sync::Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let mut processor = lite_subscription_processor_for_test(&mut runtime);
        let response = processor
            .process_request(channel, ctx, &mut request)
            .await
            .expect("processor request should succeed")
            .expect("processor should return a response");

        assert_eq!(
            ResponseCode::from(response.code()),
            ResponseCode::LiteSubscriptionQuotaExceeded
        );

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn partial_add_with_offset_option_assigns_reset_offset() {
        let mut runtime = new_test_runtime("offset-option").await;
        let inner = runtime.inner_for_test().clone();
        seed_group_config(
            &mut runtime,
            "lite-group",
            HashMap::from([(
                CheetahString::from_string(format!("+{LITE_BIND_TOPIC_ATTRIBUTE_NAME}")),
                CheetahString::from_static_str("parent-topic"),
            )]),
        );

        let dto = LiteSubscriptionDTO::new()
            .with_action(LiteSubscriptionAction::PartialAdd)
            .with_client_id(CheetahString::from_static_str("client-id"))
            .with_group(CheetahString::from_static_str("lite-group"))
            .with_topic(CheetahString::from_static_str("parent-topic"))
            .with_lite_topic_set(HashSet::from([CheetahString::from_static_str("child-a")]))
            .with_offset_option(OffsetOption::offset(12));
        let mut body = LiteSubscriptionCtlRequestBody::new();
        body.set_subscription_set(vec![dto]);
        let mut request = RemotingCommand::create_request_command(RequestCode::LiteSubscriptionCtl, EmptyHeader {})
            .set_body(Bytes::from(serde_json::to_vec(&body).expect("serialize request body")));

        let channel = create_test_channel().await;
        let ctx = std::sync::Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let mut processor = lite_subscription_processor_for_test(&mut runtime);
        let response = processor
            .process_request(channel, ctx, &mut request)
            .await
            .expect("processor request should succeed")
            .expect("processor should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let reset_offset = inner.consumer_offset_manager().query_then_erase_reset_offset(
            &CheetahString::from_string(to_lmq_name("parent-topic", "child-a").expect("convert lite topic")),
            &CheetahString::from_static_str("lite-group"),
            0,
        );
        assert_eq!(reset_offset, Some(12));

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn partial_remove_with_reset_offset_on_unsubscribe_assigns_min_offset() {
        let mut runtime = new_test_runtime("reset-on-unsubscribe").await;
        let inner = runtime.inner_for_test().clone();
        seed_group_config(
            &mut runtime,
            "lite-group",
            HashMap::from([
                (
                    CheetahString::from_string(format!("+{LITE_BIND_TOPIC_ATTRIBUTE_NAME}")),
                    CheetahString::from_static_str("parent-topic"),
                ),
                (
                    CheetahString::from_string(format!("+{LITE_SUB_RESET_OFFSET_UNSUBSCRIBE_ATTRIBUTE_NAME}")),
                    CheetahString::from_static_str("true"),
                ),
            ]),
        );
        inner.lite_subscription_registry().add_partial_subscription(
            &CheetahString::from_static_str("client-id"),
            &CheetahString::from_static_str("lite-group"),
            &CheetahString::from_static_str("parent-topic"),
            &HashSet::from([CheetahString::from_string(
                to_lmq_name("parent-topic", "child-a").expect("convert lite topic"),
            )]),
        );

        let dto = LiteSubscriptionDTO::new()
            .with_action(LiteSubscriptionAction::PartialRemove)
            .with_client_id(CheetahString::from_static_str("client-id"))
            .with_group(CheetahString::from_static_str("lite-group"))
            .with_topic(CheetahString::from_static_str("parent-topic"))
            .with_lite_topic_set(HashSet::from([CheetahString::from_static_str("child-a")]));
        let mut body = LiteSubscriptionCtlRequestBody::new();
        body.set_subscription_set(vec![dto]);
        let mut request = RemotingCommand::create_request_command(RequestCode::LiteSubscriptionCtl, EmptyHeader {})
            .set_body(Bytes::from(serde_json::to_vec(&body).expect("serialize request body")));

        let channel = create_test_channel().await;
        let ctx = std::sync::Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let mut processor = lite_subscription_processor_for_test(&mut runtime);
        let response = processor
            .process_request(channel, ctx, &mut request)
            .await
            .expect("processor request should succeed")
            .expect("processor should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let reset_offset = inner.consumer_offset_manager().query_then_erase_reset_offset(
            &CheetahString::from_string(to_lmq_name("parent-topic", "child-a").expect("convert lite topic")),
            &CheetahString::from_static_str("lite-group"),
            0,
        );
        assert_eq!(reset_offset, Some(0));

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn partial_add_in_exclusive_mode_excludes_previous_client_subscription() {
        let mut runtime = new_test_runtime("exclusive-model").await;
        let inner = runtime.inner_for_test().clone();
        seed_group_config(
            &mut runtime,
            "lite-group",
            HashMap::from([
                (
                    CheetahString::from_string(format!("+{LITE_BIND_TOPIC_ATTRIBUTE_NAME}")),
                    CheetahString::from_static_str("parent-topic"),
                ),
                (
                    CheetahString::from_string(format!("+{LITE_SUB_MODEL_ATTRIBUTE_NAME}")),
                    CheetahString::from_static_str("Exclusive"),
                ),
                (
                    CheetahString::from_string(format!("+{LITE_SUB_RESET_OFFSET_EXCLUSIVE_ATTRIBUTE_NAME}")),
                    CheetahString::from_static_str("true"),
                ),
            ]),
        );
        inner.lite_subscription_registry().add_partial_subscription(
            &CheetahString::from_static_str("client-1"),
            &CheetahString::from_static_str("lite-group"),
            &CheetahString::from_static_str("parent-topic"),
            &HashSet::from([CheetahString::from_string(
                to_lmq_name("parent-topic", "child-a").expect("convert lite topic"),
            )]),
        );

        let dto = LiteSubscriptionDTO::new()
            .with_action(LiteSubscriptionAction::PartialAdd)
            .with_client_id(CheetahString::from_static_str("client-2"))
            .with_group(CheetahString::from_static_str("lite-group"))
            .with_topic(CheetahString::from_static_str("parent-topic"))
            .with_lite_topic_set(HashSet::from([CheetahString::from_static_str("child-a")]));
        let mut body = LiteSubscriptionCtlRequestBody::new();
        body.set_subscription_set(vec![dto]);
        let mut request = RemotingCommand::create_request_command(RequestCode::LiteSubscriptionCtl, EmptyHeader {})
            .set_body(Bytes::from(serde_json::to_vec(&body).expect("serialize request body")));

        let channel = create_test_channel().await;
        let ctx = std::sync::Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let mut processor = lite_subscription_processor_for_test(&mut runtime);
        let response = processor
            .process_request(channel, ctx, &mut request)
            .await
            .expect("processor request should succeed")
            .expect("processor should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert!(inner
            .lite_subscription_registry()
            .lite_subscription(
                &CheetahString::from_static_str("client-1"),
                &CheetahString::from_static_str("lite-group"),
                &CheetahString::from_static_str("parent-topic"),
            )
            .is_none());
        let subscription = inner
            .lite_subscription_registry()
            .lite_subscription(
                &CheetahString::from_static_str("client-2"),
                &CheetahString::from_static_str("lite-group"),
                &CheetahString::from_static_str("parent-topic"),
            )
            .expect("new exclusive subscriber should be retained");
        assert!(subscription.lite_topic_set().contains(&CheetahString::from_string(
            to_lmq_name("parent-topic", "child-a").expect("convert lite topic")
        )));
        let reset_offset = inner.consumer_offset_manager().query_then_erase_reset_offset(
            &CheetahString::from_string(to_lmq_name("parent-topic", "child-a").expect("convert lite topic")),
            &CheetahString::from_static_str("lite-group"),
            0,
        );
        assert_eq!(reset_offset, Some(0));

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }
}
