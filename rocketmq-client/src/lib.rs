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

#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::result_large_err)]
#![recursion_limit = "256"]

extern crate core;

// Define macros at crate root so they're available throughout the crate
/// Create a client error with optional response code
#[macro_export]
macro_rules! mq_client_err {
    // Handle errors with a custom ResponseCode and formatted string
    ($response_code:expr, $fmt:expr, $($arg:expr),*) => {{
        let formatted_msg = format!($fmt, $($arg),*);
        let error_message = format!("CODE: {}  DESC: {}", $response_code as i32, formatted_msg);
        let faq_msg = rocketmq_common::common::FAQUrl::attach_default_url(Some(error_message.as_str()));
        rocketmq_error::RocketMQError::illegal_argument(faq_msg)
    }};

    ($response_code:expr, $error_message:expr) => {{
        let error_message = format!("CODE: {}  DESC: {}", $response_code as i32, $error_message);
        let faq_msg = rocketmq_common::common::FAQUrl::attach_default_url(Some(error_message.as_str()));
        rocketmq_error::RocketMQError::illegal_argument(faq_msg)
    }};

    // Handle errors without a ResponseCode, using only the error message (accepts both &str and String)
    ($error_message:expr) => {{
        let error_msg = format!("{}", $error_message);
        let faq_msg = rocketmq_common::common::FAQUrl::attach_default_url(Some(error_msg.as_str()));
        rocketmq_error::RocketMQError::illegal_argument(faq_msg)
    }};
}

/// Create a broker operation error
#[macro_export]
macro_rules! client_broker_err {
    // Handle errors with a custom ResponseCode and formatted string
    ($response_code:expr, $error_message:expr, $broker_addr:expr) => {{
        rocketmq_error::RocketMQError::broker_operation_failed(
            "BROKER_OPERATION",
            $response_code as i32,
            $error_message,
        )
        .with_broker_addr($broker_addr)
    }};
    // Handle errors without a ResponseCode, using only the error message
    ($response_code:expr, $error_message:expr) => {{
        rocketmq_error::RocketMQError::broker_operation_failed(
            "BROKER_OPERATION",
            $response_code as i32,
            $error_message,
        )
    }};
}

pub mod admin;
pub mod base;
pub mod common;
pub mod consumer;
pub mod exception;
pub mod factory;
mod hook;
pub mod implementation;
pub mod latency;
pub mod legacy;
pub mod lock;
pub mod producer;
mod runtime;
pub mod stat;
mod trace;
mod types;
pub mod utils;

pub use crate::admin::DefaultMQAdminExt;
pub use crate::admin::DefaultMQAdminExtImpl;
pub use crate::admin::MQAdminExt;
pub use crate::admin::MQAdminExtInner;
pub use crate::admin::MQAdminExtInnerImpl;
pub use crate::base::MQAdmin;
pub use crate::base::MqClientAdmin;
pub use crate::base::MqClientAdminInner;
pub use crate::common::acl::AclConstants;
pub use crate::common::acl::AclException;
pub use crate::common::acl::AclSigner;
pub use crate::common::acl::AclUtils;
pub use crate::common::acl::Permission;
pub use crate::common::acl::SigningAlgorithm;
pub use crate::common::acl_client_rpc_hook::AclClientRPCHook;
pub use crate::common::admin_tool_result::AdminToolResult;
pub use crate::common::admin_tools_result_code_enum::AdminToolsResultCodeEnum;
pub use crate::common::nameserver_access_config::NameserverAccessConfig;
pub use crate::common::session_credentials::SessionCredentials;

/// Compatibility exports used by the admin adapter while legacy public
/// signatures are retired. New consumers should depend on the canonical owner
/// crates directly.
#[doc(hidden)]
pub mod admin_adapter_compat {
    pub use rocketmq_error as error;
    pub use rocketmq_remoting as remoting;
    pub use rocketmq_rust as runtime;

    pub mod message {
        pub use rocketmq_common::common::message::message_ext::MessageExt;
        pub use rocketmq_common::common::message::message_single::Message;
        pub use rocketmq_common::common::message::MessageTrait;
    }
}

/// Compatibility surface used by the Proxy Cluster adapter while Client
/// runtime signatures are narrowed to canonical protocol contracts.
#[doc(hidden)]
pub mod proxy_adapter_compat {
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::sync::LazyLock;
    use std::sync::Mutex;

    use cheetah_string::CheetahString;
    use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
    use rocketmq_remoting::runtime::RPCHook;
    use rocketmq_security_api::OutboundSigner;
    use rocketmq_security_api::SecurityRequestView;

    pub use rocketmq_common::common::attribute::topic_message_type::TopicMessageType;
    pub use rocketmq_common::common::boundary_type::BoundaryType;
    pub use rocketmq_common::common::filter::expression_type::ExpressionType;
    pub use rocketmq_common::common::message::message_ext::MessageExt;
    pub use rocketmq_common::common::message::message_id::MessageId;
    pub use rocketmq_common::common::message::message_queue::MessageQueue;
    pub use rocketmq_common::common::message::message_queue_assignment::MessageQueueAssignment;
    pub use rocketmq_common::common::message::message_single::Message;
    pub use rocketmq_common::common::message::MessageConst;
    pub use rocketmq_common::common::message::MessageTrait;
    pub use rocketmq_common::common::mix_all::LOGICAL_QUEUE_MOCK_BROKER_PREFIX;
    pub use rocketmq_common::common::mix_all::MASTER_ID;
    pub use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
    pub use rocketmq_common::common::sys_flag::pull_sys_flag::PullSysFlag;
    pub use rocketmq_common::MessageDecoder;
    pub use rocketmq_common::TimeUtils::current_millis;
    pub use rocketmq_remoting::protocol::route_facade::BrokerDataExt;

    pub type ClientRpcHook = dyn RPCHook;

    /// Opaque compatibility handle for the Client instance owned by the Proxy
    /// Cluster worker.
    ///
    /// The standard shared owner remains private to Client; the Cluster adapter
    /// can only use the instance API and keeps cloned handles on its managed worker.
    pub struct ClientInstanceHandle {
        inner: Arc<crate::factory::mq_client_instance::MQClientInstance>,
        owner: ManagedClientOwner,
    }

    #[derive(Clone, Eq, Hash, PartialEq)]
    struct ManagedClientKey {
        client_id: CheetahString,
    }

    #[derive(Eq, PartialEq)]
    struct ManagedClientIdentity {
        namesrv_addr: Option<CheetahString>,
        use_tls: bool,
        vip_channel_enabled: bool,
        api_timeout_millis: u64,
        rpc_hook_identity: Option<usize>,
    }

    struct ManagedClientEntry {
        identity: ManagedClientIdentity,
        owners: AtomicUsize,
        operation_gate: Arc<tokio::sync::Mutex<()>>,
    }

    struct ManagedClientOwner {
        key: ManagedClientKey,
        entry: Arc<ManagedClientEntry>,
        released: AtomicBool,
    }

    static MANAGED_CLIENTS: LazyLock<dashmap::DashMap<ManagedClientKey, Arc<ManagedClientEntry>>> =
        LazyLock::new(dashmap::DashMap::new);

    /// Isolates a managed Client owner in the global Client manager while
    /// preserving the caller-provided instance name as a readable prefix.
    pub fn client_config_for_managed_domain(
        domain_id: u64,
        mut client_config: crate::base::client_config::ClientConfig,
    ) -> crate::base::client_config::ClientConfig {
        client_config.set_instance_name(CheetahString::from_string(format!(
            "{}#managed-domain-{domain_id}",
            client_config.instance_name
        )));
        client_config
    }

    impl ClientInstanceHandle {
        pub fn get_or_create(
            domain_id: u64,
            client_config: crate::base::client_config::ClientConfig,
            rpc_hook: Option<Arc<ClientRpcHook>>,
        ) -> rocketmq_error::RocketMQResult<Self> {
            let client_config = client_config_for_managed_domain(domain_id, client_config);
            let client_id = CheetahString::from_string(client_config.build_mq_client_id());
            let key = ManagedClientKey { client_id };
            let identity = ManagedClientIdentity {
                namesrv_addr: client_config.namesrv_addr.clone(),
                use_tls: client_config.use_tls,
                vip_channel_enabled: client_config.vip_channel_enabled,
                api_timeout_millis: client_config.mq_client_api_timeout,
                rpc_hook_identity: rpc_hook.as_ref().map(|hook| Arc::as_ptr(hook).cast::<()>() as usize),
            };
            let entry = match MANAGED_CLIENTS.entry(key.clone()) {
                dashmap::mapref::entry::Entry::Occupied(entry) => {
                    if entry.get().identity != identity {
                        return Err(rocketmq_error::RocketMQError::IllegalArgument(
                            "managed Client instance configuration conflicts with an existing owner".to_owned(),
                        ));
                    }
                    entry.get().owners.fetch_add(1, Ordering::AcqRel);
                    entry.get().clone()
                }
                dashmap::mapref::entry::Entry::Vacant(entry) => {
                    let managed = Arc::new(ManagedClientEntry {
                        identity,
                        owners: AtomicUsize::new(1),
                        operation_gate: Arc::new(tokio::sync::Mutex::new(())),
                    });
                    entry.insert(managed.clone());
                    managed
                }
            };
            let inner = crate::implementation::mq_client_manager::MQClientManager::get_instance()
                .get_or_create_mq_client_instance(client_config, rpc_hook);
            Ok(Self {
                inner,
                owner: ManagedClientOwner {
                    key,
                    entry,
                    released: AtomicBool::new(false),
                },
            })
        }

        pub async fn start(&self) -> rocketmq_error::RocketMQResult<()> {
            let _operation = self.operation().await;
            self.inner.start().await
        }

        pub async fn topic_route(
            &self,
            topic: &str,
            timeout_millis: u64,
        ) -> rocketmq_error::RocketMQResult<Option<rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData>>
        {
            let _operation = self.operation().await;
            self.inner
                .get_mq_client_api_impl()?
                .get_topic_route_info_from_name_server(topic, timeout_millis)
                .await
        }

        pub async fn lock_batch_mq(
            &self,
            broker_addr: &str,
            request: rocketmq_protocol::protocol::body::request::lock_batch_request_body::LockBatchRequestBody,
            timeout_millis: u64,
        ) -> rocketmq_error::RocketMQResult<std::collections::HashSet<MessageQueue>> {
            let _operation = self.operation().await;
            self.inner
                .get_mq_client_api_impl()?
                .lock_batch_mq(broker_addr, request, timeout_millis)
                .await
        }

        pub async fn unlock_batch_mq(
            &self,
            broker_addr: &CheetahString,
            request: rocketmq_protocol::protocol::body::unlock_batch_request_body::UnlockBatchRequestBody,
            timeout_millis: u64,
        ) -> rocketmq_error::RocketMQResult<()> {
            let _operation = self.operation().await;
            self.inner
                .get_mq_client_api_impl()?
                .unlock_batch_mq(broker_addr, request, timeout_millis, false)
                .await
        }

        #[allow(
            clippy::too_many_arguments,
            reason = "mirrors the RocketMQ query-assignment wire contract"
        )]
        pub async fn query_assignment(
            &self,
            broker_addr: &CheetahString,
            topic: CheetahString,
            consumer_group: CheetahString,
            client_id: CheetahString,
            strategy_name: CheetahString,
            message_model: rocketmq_protocol::protocol::heartbeat::message_model::MessageModel,
            timeout_millis: u64,
        ) -> rocketmq_error::RocketMQResult<Option<Vec<MessageQueueAssignment>>> {
            let _operation = self.operation().await;
            self.inner
                .get_mq_client_api_impl()?
                .query_assignment(
                    broker_addr,
                    topic,
                    consumer_group,
                    client_id,
                    strategy_name,
                    message_model,
                    timeout_millis,
                )
                .await
                .map(|assignments| assignments.map(|items| items.into_iter().collect()))
        }

        pub async fn pop_message(
            &self,
            broker_name: &CheetahString,
            broker_addr: &CheetahString,
            request: rocketmq_protocol::protocol::header::pop_message_request_header::PopMessageRequestHeader,
            timeout_millis: u64,
        ) -> rocketmq_error::RocketMQResult<crate::consumer::pop_result::PopResult> {
            let _operation = self.operation().await;
            let (sender, receiver) = tokio::sync::oneshot::channel();
            self.inner
                .get_mq_client_api_impl()?
                .pop_message_async(
                    broker_name,
                    broker_addr,
                    request,
                    timeout_millis,
                    OwnedPopCallback { sender: Some(sender) },
                )
                .await?;
            receiver.await.unwrap_or_else(|_| {
                Err(rocketmq_error::RocketMQError::Internal(
                    "Client pop callback closed without a result".to_owned(),
                ))
            })
        }

        pub async fn ack_message(
            &self,
            broker_addr: &CheetahString,
            request: rocketmq_protocol::protocol::header::ack_message_request_header::AckMessageRequestHeader,
            timeout_millis: u64,
        ) -> rocketmq_error::RocketMQResult<crate::consumer::ack_result::AckResult> {
            let _operation = self.operation().await;
            let (sender, receiver) = tokio::sync::oneshot::channel();
            self.inner
                .get_mq_client_api_impl()?
                .ack_message_async(broker_addr, request, timeout_millis, OwnedAckCallback::new(sender))
                .await?;
            receiver.await.unwrap_or_else(|_| {
                Err(rocketmq_error::RocketMQError::Internal(
                    "Client ack callback closed without a result".to_owned(),
                ))
            })
        }

        pub async fn change_invisible_time(
            &self,
            broker_name: &CheetahString,
            broker_addr: &CheetahString,
            request: rocketmq_protocol::protocol::header::change_invisible_time_request_header::ChangeInvisibleTimeRequestHeader,
            timeout_millis: u64,
        ) -> rocketmq_error::RocketMQResult<crate::consumer::ack_result::AckResult> {
            let _operation = self.operation().await;
            let (sender, receiver) = tokio::sync::oneshot::channel();
            self.inner
                .get_mq_client_api_impl()?
                .change_invisible_time_async(
                    broker_name,
                    broker_addr,
                    request,
                    timeout_millis,
                    OwnedAckCallback::new(sender),
                )
                .await?;
            receiver.await.unwrap_or_else(|_| {
                Err(rocketmq_error::RocketMQError::Internal(
                    "Client change-invisible callback closed without a result".to_owned(),
                ))
            })
        }

        pub async fn end_transaction(
            &self,
            broker_addr: &CheetahString,
            request: rocketmq_protocol::protocol::header::end_transaction_request_header::EndTransactionRequestHeader,
            remark: CheetahString,
            timeout_millis: u64,
        ) -> rocketmq_error::RocketMQResult<()> {
            let _operation = self.operation().await;
            self.inner
                .get_mq_client_api_impl()?
                .end_transaction_oneway(broker_addr, request, remark, timeout_millis)
                .await
        }

        pub async fn find_subscribe_broker_addr(
            &self,
            broker_name: &CheetahString,
            broker_id: u64,
            only_this_broker: bool,
        ) -> Option<CheetahString> {
            let _operation = self.operation().await;
            let instance = self.inner.clone();
            instance
                .find_broker_address_in_subscribe(broker_name, broker_id, only_this_broker)
                .await
                .map(|found| found.broker_addr)
        }

        pub async fn refresh_topic_route(&self, topic: &CheetahString) -> bool {
            let _operation = self.operation().await;
            let instance = self.inner.clone();
            instance.update_topic_route_info_from_name_server_topic(topic).await
        }

        pub async fn broker_name_for_queue(&self, queue: &MessageQueue) -> CheetahString {
            let _operation = self.operation().await;
            self.inner.get_broker_name_from_message_queue(queue).await
        }

        pub async fn pull_outcome_from_broker(
            &self,
            broker_addr: &str,
            request: rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader,
            timeout_millis: u64,
        ) -> rocketmq_error::RocketMQResult<rocketmq_model::result::PullOutcome<MessageExt>> {
            let _operation = self.operation().await;
            let result = self
                .inner
                .pull_message_from_broker(broker_addr, request, timeout_millis)
                .await?;
            Ok((&result).into())
        }

        #[allow(clippy::too_many_arguments, reason = "mirrors the RocketMQ send-back wire contract")]
        pub async fn consumer_send_message_back(
            &self,
            broker_addr: &str,
            broker_name: Option<&str>,
            message: &MessageExt,
            consumer_group: &str,
            delay_level: i32,
            timeout_millis: u64,
            max_consume_retry_times: i32,
        ) -> rocketmq_error::RocketMQResult<()> {
            let _operation = self.operation().await;
            self.inner
                .consumer_send_message_back(
                    broker_addr,
                    broker_name,
                    message,
                    consumer_group,
                    delay_level,
                    timeout_millis,
                    max_consume_retry_times,
                )
                .await
        }

        pub async fn update_consumer_offset(
            &self,
            broker_addr: &CheetahString,
            request: rocketmq_protocol::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader,
            timeout_millis: u64,
        ) -> rocketmq_error::RocketMQResult<()> {
            let _operation = self.operation().await;
            self.inner
                .update_consumer_offset(broker_addr, request, timeout_millis)
                .await
        }

        pub async fn query_consumer_offset(
            &self,
            broker_addr: &str,
            request: rocketmq_protocol::protocol::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader,
            timeout_millis: u64,
        ) -> rocketmq_error::RocketMQResult<i64> {
            let _operation = self.operation().await;
            self.inner
                .query_consumer_offset(broker_addr, request, timeout_millis)
                .await
        }

        pub async fn min_offset(
            &self,
            broker_addr: &str,
            queue: &MessageQueue,
            timeout_millis: u64,
        ) -> rocketmq_error::RocketMQResult<i64> {
            let _operation = self.operation().await;
            self.inner.get_min_offset(broker_addr, queue, timeout_millis).await
        }

        pub async fn max_offset(
            &self,
            broker_addr: &str,
            queue: &MessageQueue,
            timeout_millis: u64,
        ) -> rocketmq_error::RocketMQResult<i64> {
            let _operation = self.operation().await;
            self.inner.get_max_offset(broker_addr, queue, timeout_millis).await
        }

        pub async fn search_offset(
            &self,
            broker_addr: &str,
            queue: &MessageQueue,
            timestamp: i64,
            boundary_type: BoundaryType,
            timeout_millis: u64,
        ) -> rocketmq_error::RocketMQResult<i64> {
            let _operation = self.operation().await;
            self.inner
                .search_offset_by_timestamp(broker_addr, queue, timestamp, boundary_type, timeout_millis)
                .await
        }

        pub async fn topic_config(
            &self,
            broker_addr: &CheetahString,
            topic: CheetahString,
            timeout_millis: u64,
        ) -> rocketmq_error::RocketMQResult<rocketmq_common::common::config::TopicConfig> {
            let _operation = self.operation().await;
            self.inner.get_topic_config(broker_addr, topic, timeout_millis).await
        }

        pub async fn subscription_group_config(
            &self,
            broker_addr: &CheetahString,
            group: CheetahString,
            timeout_millis: u64,
        ) -> rocketmq_error::RocketMQResult<
            rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig,
        > {
            let _operation = self.operation().await;
            self.inner
                .get_subscription_group_config(broker_addr, group, timeout_millis)
                .await
        }

        pub async fn broker_cluster_info(
            &self,
            timeout_millis: u64,
        ) -> rocketmq_error::RocketMQResult<rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo>
        {
            let _operation = self.operation().await;
            self.inner.get_broker_cluster_info(timeout_millis).await
        }

        pub async fn user(
            &self,
            broker_addr: CheetahString,
            username: CheetahString,
            timeout_millis: u64,
        ) -> rocketmq_error::RocketMQResult<Option<rocketmq_protocol::protocol::body::user_info::UserInfo>> {
            let _operation = self.operation().await;
            self.inner.get_user(broker_addr, username, timeout_millis).await
        }

        pub async fn acl(
            &self,
            broker_addr: CheetahString,
            subject: CheetahString,
            timeout_millis: u64,
        ) -> rocketmq_error::RocketMQResult<Option<rocketmq_protocol::protocol::body::acl_info::AclInfo>> {
            let _operation = self.operation().await;
            self.inner.get_acl(broker_addr, subject, timeout_millis).await
        }

        pub async fn shutdown_owned(&self) {
            if !self.release_managed() {
                return;
            }
            let _operation = self.operation().await;
            self.inner.shutdown().await;
        }

        fn release_managed(&self) -> bool {
            if self.owner.released.swap(true, Ordering::AcqRel) {
                return false;
            }
            let dashmap::mapref::entry::Entry::Occupied(entry) = MANAGED_CLIENTS.entry(self.owner.key.clone()) else {
                return false;
            };
            if !Arc::ptr_eq(entry.get(), &self.owner.entry) {
                return false;
            }
            let owners = entry.get().owners.load(Ordering::Acquire);
            if owners == 0 {
                return false;
            }
            entry.get().owners.store(owners - 1, Ordering::Release);
            if owners != 1 {
                return false;
            }

            let expected = Arc::as_ptr(&self.inner).cast::<()>();
            crate::implementation::mq_client_manager::MQClientManager::get_instance()
                .remove_client_factory_if_same(&self.owner.key.client_id, expected);
            entry.remove();
            true
        }

        async fn operation(&self) -> tokio::sync::OwnedMutexGuard<()> {
            self.owner.entry.operation_gate.clone().lock_owned().await
        }
    }

    impl Drop for ClientInstanceHandle {
        fn drop(&mut self) {
            if self.release_managed() {
                tracing::warn!(
                    client_id = %self.owner.key.client_id,
                    "managed Client handle dropped before asynchronous shutdown completed"
                );
            }
        }
    }

    struct OwnedPopCallback {
        sender: Option<
            tokio::sync::oneshot::Sender<rocketmq_error::RocketMQResult<crate::consumer::pop_result::PopResult>>,
        >,
    }

    impl crate::consumer::pop_callback::PopCallback for OwnedPopCallback {
        async fn on_success(&mut self, pop_result: crate::consumer::pop_result::PopResult) {
            if let Some(sender) = self.sender.take() {
                let _ = sender.send(Ok(pop_result));
            }
        }

        fn on_error(&mut self, error: rocketmq_error::RocketMQError) {
            if let Some(sender) = self.sender.take() {
                let _ = sender.send(Err(error));
            }
        }
    }

    struct OwnedAckCallback {
        sender: Mutex<
            Option<
                tokio::sync::oneshot::Sender<rocketmq_error::RocketMQResult<crate::consumer::ack_result::AckResult>>,
            >,
        >,
    }

    impl OwnedAckCallback {
        fn new(
            sender: tokio::sync::oneshot::Sender<
                rocketmq_error::RocketMQResult<crate::consumer::ack_result::AckResult>,
            >,
        ) -> Self {
            Self {
                sender: Mutex::new(Some(sender)),
            }
        }

        fn send(&self, result: rocketmq_error::RocketMQResult<crate::consumer::ack_result::AckResult>) {
            let mut sender = match self.sender.lock() {
                Ok(sender) => sender,
                Err(poisoned) => poisoned.into_inner(),
            };
            if let Some(sender) = sender.take() {
                let _ = sender.send(result);
            }
        }
    }

    impl crate::consumer::ack_callback::AckCallback for OwnedAckCallback {
        fn on_success(&self, ack_result: crate::consumer::ack_result::AckResult) {
            self.send(Ok(ack_result));
        }

        fn on_exception(&self, error: rocketmq_error::RocketMQError) {
            self.send(Err(error));
        }
    }

    pub fn rpc_hook_from_outbound_signer(signer: Arc<dyn OutboundSigner>) -> Arc<ClientRpcHook> {
        Arc::new(OutboundSignerRpcHook { signer })
    }

    struct OutboundSignerRpcHook {
        signer: Arc<dyn OutboundSigner>,
    }

    impl RPCHook for OutboundSignerRpcHook {
        fn do_before_request(
            &self,
            _remote_addr: SocketAddr,
            request: &mut RemotingCommand,
        ) -> rocketmq_error::RocketMQResult<()> {
            // RPCHook callers normally serialize the custom header first, but
            // keep the adapter correct when invoked directly or by another
            // Client transport path.
            request.make_custom_header_to_net();
            let empty_fields = HashMap::new();
            let fields = request.ext_fields().unwrap_or(&empty_fields);
            let signature = self
                .signer
                .sign(SecurityRequestView::new(
                    request.code(),
                    request.version(),
                    fields,
                    request.body().map(AsRef::as_ref),
                    None,
                ))
                .map_err(|_| rocketmq_error::RocketMQError::authentication_failed("outbound request signing failed"))?;

            request.ensure_ext_fields_initialized();
            for (key, value) in signature.fields() {
                request.add_ext_field(key.clone(), value.expose_secret().clone());
            }
            Ok(())
        }

        fn do_after_response(
            &self,
            _remote_addr: SocketAddr,
            _request: &RemotingCommand,
            _response: &mut RemotingCommand,
        ) -> rocketmq_error::RocketMQResult<()> {
            Ok(())
        }
    }

    #[cfg(test)]
    mod tests {
        use std::sync::Arc;

        use super::client_config_for_managed_domain;
        use super::rpc_hook_from_outbound_signer;
        use super::CheetahString;
        use super::ClientInstanceHandle;
        use super::ManagedClientKey;
        use super::OutboundSigner;
        use super::RemotingCommand;
        use super::SecurityRequestView;
        use super::MANAGED_CLIENTS;
        use rocketmq_protocol::code::request_code::RequestCode;
        use rocketmq_protocol::protocol::header::client_request_header::GetRouteInfoRequestHeader;
        use rocketmq_security_api::Secret;
        use rocketmq_security_api::Signature;
        use rocketmq_security_api::SigningError;

        struct FixedSigner;

        impl OutboundSigner for FixedSigner {
            fn sign(&self, request: SecurityRequestView<'_>) -> Result<Signature, SigningError> {
                assert_eq!(
                    request.fields().get("topic").map(|value| value.as_str()),
                    Some("TopicA")
                );
                Ok(Signature::new(vec![
                    ("AccessKey".into(), Secret::new("cluster-client".into())),
                    ("Signature".into(), Secret::new("redacted-signature".into())),
                ]))
            }
        }

        struct FailingSigner;

        impl OutboundSigner for FailingSigner {
            fn sign(&self, _request: SecurityRequestView<'_>) -> Result<Signature, SigningError> {
                Err(SigningError::Failed(CheetahString::from("secret signing diagnostic")))
            }
        }

        #[test]
        fn outbound_signer_adapter_applies_fields_to_client_rpc_request() {
            let hook = rpc_hook_from_outbound_signer(Arc::new(FixedSigner));
            let mut request = RemotingCommand::create_request_command(
                RequestCode::GetRouteinfoByTopic,
                GetRouteInfoRequestHeader::new("TopicA", None),
            );

            hook.do_before_request("127.0.0.1:9876".parse().unwrap(), &mut request)
                .unwrap();

            let fields = request.ext_fields().unwrap();
            assert_eq!(
                fields.get("AccessKey").map(|value| value.as_str()),
                Some("cluster-client")
            );
            assert_eq!(
                fields.get("Signature").map(|value| value.as_str()),
                Some("redacted-signature")
            );
        }

        #[test]
        fn outbound_signer_adapter_redacts_signer_failures() {
            let hook = rpc_hook_from_outbound_signer(Arc::new(FailingSigner));
            let mut request = RemotingCommand::create_request_command(
                RequestCode::GetRouteinfoByTopic,
                GetRouteInfoRequestHeader::new("TopicA", None),
            );

            let error = hook
                .do_before_request("127.0.0.1:9876".parse().unwrap(), &mut request)
                .expect_err("signing failure must fail closed");
            let message = error.to_string();
            assert!(message.contains("outbound request signing failed"));
            assert!(!message.contains("secret signing diagnostic"));
        }

        #[tokio::test]
        async fn managed_client_is_shared_until_the_last_owner_releases_it() {
            let domain_id = 71_001;
            let mut config = crate::base::client_config::ClientConfig::default();
            config.set_instance_name("managed-client-shared".into());
            let client_id = client_config_for_managed_domain(domain_id, config.clone())
                .build_mq_client_id()
                .into();
            let key = ManagedClientKey { client_id };

            let first = ClientInstanceHandle::get_or_create(domain_id, config.clone(), None).unwrap();
            let second = ClientInstanceHandle::get_or_create(domain_id, config, None).unwrap();

            assert!(Arc::ptr_eq(&first.inner, &second.inner));
            assert_eq!(
                MANAGED_CLIENTS
                    .get(&key)
                    .unwrap()
                    .owners
                    .load(std::sync::atomic::Ordering::Acquire),
                2
            );

            first.shutdown_owned().await;
            assert_eq!(
                MANAGED_CLIENTS
                    .get(&key)
                    .unwrap()
                    .owners
                    .load(std::sync::atomic::Ordering::Acquire),
                1
            );

            second.shutdown_owned().await;
            assert!(!MANAGED_CLIENTS.contains_key(&key));
        }

        #[tokio::test]
        async fn managed_client_rejects_conflicting_configuration_for_the_same_identity() {
            let domain_id = 71_002;
            let mut first_config = crate::base::client_config::ClientConfig::default();
            first_config.set_instance_name("managed-client-conflict".into());
            first_config.set_namesrv_addr("127.0.0.1:9876".into());
            let mut second_config = first_config.clone();
            second_config.set_namesrv_addr("127.0.0.2:9876".into());

            let first = ClientInstanceHandle::get_or_create(domain_id, first_config, None).unwrap();
            let error = ClientInstanceHandle::get_or_create(domain_id, second_config, None)
                .err()
                .expect("conflicting managed configuration must fail closed");

            assert!(matches!(error, rocketmq_error::RocketMQError::IllegalArgument(_)));
            first.shutdown_owned().await;
        }

        #[tokio::test]
        async fn managed_client_isolated_across_runtime_domains() {
            let mut config = crate::base::client_config::ClientConfig::default();
            config.set_instance_name("managed-client-cross-domain".into());
            let first_domain = 71_004;
            let second_domain = 71_005;
            let first_key = ManagedClientKey {
                client_id: client_config_for_managed_domain(first_domain, config.clone())
                    .build_mq_client_id()
                    .into(),
            };
            let second_key = ManagedClientKey {
                client_id: client_config_for_managed_domain(second_domain, config.clone())
                    .build_mq_client_id()
                    .into(),
            };
            let first = ClientInstanceHandle::get_or_create(first_domain, config.clone(), None).unwrap();
            let second = ClientInstanceHandle::get_or_create(second_domain, config, None).unwrap();

            assert!(!Arc::ptr_eq(&first.inner, &second.inner));
            assert_eq!(
                MANAGED_CLIENTS
                    .get(&first_key)
                    .unwrap()
                    .owners
                    .load(std::sync::atomic::Ordering::Acquire),
                1
            );
            assert_eq!(
                MANAGED_CLIENTS
                    .get(&second_key)
                    .unwrap()
                    .owners
                    .load(std::sync::atomic::Ordering::Acquire),
                1
            );

            first.shutdown_owned().await;
            assert!(!MANAGED_CLIENTS.contains_key(&first_key));
            assert!(MANAGED_CLIENTS.contains_key(&second_key));
            second.shutdown_owned().await;
            assert!(!MANAGED_CLIENTS.contains_key(&second_key));
        }

        #[test]
        fn dropping_managed_handles_releases_the_global_lease() {
            let domain_id = 71_006;
            let mut config = crate::base::client_config::ClientConfig::default();
            config.set_instance_name("managed-client-drop-release".into());
            let client_id = client_config_for_managed_domain(domain_id, config.clone())
                .build_mq_client_id()
                .into();
            let key = ManagedClientKey { client_id };
            let first = ClientInstanceHandle::get_or_create(domain_id, config.clone(), None).unwrap();
            let second = ClientInstanceHandle::get_or_create(domain_id, config, None).unwrap();

            drop(first);
            assert_eq!(
                MANAGED_CLIENTS
                    .get(&key)
                    .unwrap()
                    .owners
                    .load(std::sync::atomic::Ordering::Acquire),
                1
            );
            drop(second);
            assert!(!MANAGED_CLIENTS.contains_key(&key));
        }

        #[tokio::test]
        async fn releasing_the_last_lease_prevents_manager_aba_reuse() {
            let mut config = crate::base::client_config::ClientConfig::default();
            config.set_instance_name("managed-client-aba".into());
            let first = ClientInstanceHandle::get_or_create(71_008, config.clone(), None).unwrap();
            first.shutdown_owned().await;

            let second = ClientInstanceHandle::get_or_create(71_008, config, None).unwrap();
            assert!(!Arc::ptr_eq(&first.inner, &second.inner));
            second.shutdown_owned().await;
        }
    }
}
#[doc(hidden)]
pub use crate::consumer::consumer_impl::consume_message_concurrently_service::run_concurrent_clean_expire_lifecycle_probe;
#[doc(hidden)]
pub use crate::consumer::consumer_impl::consume_message_concurrently_service::ConcurrentCleanExpireLifecycleProbe;
#[doc(hidden)]
pub use crate::consumer::consumer_impl::consume_message_orderly_service::run_orderly_lock_periodic_lifecycle_probe;
#[doc(hidden)]
pub use crate::consumer::consumer_impl::consume_message_orderly_service::OrderlyLockPeriodicLifecycleProbe;
#[doc(hidden)]
pub use crate::consumer::consumer_impl::consume_message_pop_orderly_service::run_pop_orderly_lock_refresh_lifecycle_probe;
#[doc(hidden)]
pub use crate::consumer::consumer_impl::consume_message_pop_orderly_service::PopOrderlyLockRefreshLifecycleProbe;
#[doc(hidden)]
pub use crate::consumer::consumer_impl::default_lite_pull_consumer_impl::run_lite_pull_task_lifecycle_probe;
#[doc(hidden)]
pub use crate::consumer::consumer_impl::default_lite_pull_consumer_impl::LitePullTaskLifecycleProbe;
#[doc(hidden)]
pub use crate::consumer::consumer_impl::pull_message_service::run_pull_message_service_lifecycle_probe;
#[doc(hidden)]
pub use crate::consumer::consumer_impl::pull_message_service::PullMessageServiceLifecycleProbe;
pub use crate::consumer::consumer_impl::pull_request_ext::PullResultExt;
#[doc(hidden)]
pub use crate::consumer::consumer_impl::re_balance::rebalance_service::run_rebalance_service_lifecycle_probe;
#[doc(hidden)]
pub use crate::consumer::consumer_impl::re_balance::rebalance_service::RebalanceServiceLifecycleProbe;
pub use crate::consumer::notify_result::NotifyResult;
#[doc(hidden)]
pub use crate::consumer::store::local_file_offset_store::run_local_file_offset_store_lifecycle_probe;
#[doc(hidden)]
pub use crate::consumer::store::local_file_offset_store::LocalFileOffsetStoreLifecycleProbe;
pub use crate::consumer::AbstractAllocateMessageQueueStrategy;
pub use crate::consumer::AckCallback;
pub use crate::consumer::AckCallbackFn;
pub use crate::consumer::AckResult;
pub use crate::consumer::AckStatus;
pub use crate::consumer::AllocateMachineRoomNearby;
pub use crate::consumer::AllocateMessageQueueAveragely;
pub use crate::consumer::AllocateMessageQueueAveragelyByCircle;
pub use crate::consumer::AllocateMessageQueueByConfig;
pub use crate::consumer::AllocateMessageQueueByMachineRoom;
pub use crate::consumer::AllocateMessageQueueByMachineRoomNearby;
pub use crate::consumer::AllocateMessageQueueConsistentHash;
pub use crate::consumer::AllocateMessageQueueStrategy;
pub use crate::consumer::ArcMessageQueueListener;
pub use crate::consumer::ConsumeConcurrentlyContext;
pub use crate::consumer::ConsumeConcurrentlyStatus;
pub use crate::consumer::ConsumeOrderlyContext;
pub use crate::consumer::ConsumeOrderlyStatus;
pub use crate::consumer::ConsumerTuningProfile;
pub use crate::consumer::ControllableOffset;
pub use crate::consumer::DefaultLitePullConsumer;
pub use crate::consumer::DefaultLitePullConsumerBuilder;
pub use crate::consumer::DefaultMQPushConsumer;
pub use crate::consumer::DefaultMQPushConsumerBuilder;
pub use crate::consumer::HashFunction;
pub use crate::consumer::LitePullConsumer;
pub use crate::consumer::LocalFileOffsetStore;
pub use crate::consumer::MQConsumer;
pub use crate::consumer::MQConsumerInner;
pub use crate::consumer::MQPushConsumer;
pub use crate::consumer::MachineRoomResolver;
pub use crate::consumer::MessageListener;
pub use crate::consumer::MessageListenerConcurrently;
pub use crate::consumer::MessageListenerOrderly;
pub use crate::consumer::MessageQueueListener;
pub use crate::consumer::MessageSelector;
pub use crate::consumer::OffsetSerialize;
pub use crate::consumer::OffsetSerializeWrapper;
pub use crate::consumer::OffsetStore;
pub use crate::consumer::PopCallback;
pub use crate::consumer::PopCallbackFn;
pub use crate::consumer::PopResult;
pub use crate::consumer::PopStatus;
pub use crate::consumer::PullCallback;
pub use crate::consumer::PullCallbackFn;
pub use crate::consumer::PullResult;
pub use crate::consumer::PullStatus;
pub use crate::consumer::ReadOffsetType;
pub use crate::consumer::RemoteBrokerOffsetStore;
pub use crate::consumer::TopicMessageQueueChangeListener;
pub use crate::exception::MQBrokerException;
pub use crate::exception::MQClientException;
pub use crate::exception::OffsetNotFoundException;
pub use crate::exception::RequestTimeoutException;
#[doc(hidden)]
pub use crate::factory::mq_client_instance::run_connection_event_listener_lifecycle_probe;
#[doc(hidden)]
pub use crate::factory::mq_client_instance::run_heartbeat_route_index_probe;
#[doc(hidden)]
pub use crate::factory::mq_client_instance::run_route_refresh_concurrent_stale_guard_probe;
#[doc(hidden)]
pub use crate::factory::mq_client_instance::run_route_refresh_shard_probe;
#[doc(hidden)]
pub use crate::factory::mq_client_instance::ConnectionEventListenerLifecycleProbe;
#[doc(hidden)]
pub use crate::factory::mq_client_instance::HeartbeatRouteIndexProbe;
#[doc(hidden)]
pub use crate::factory::mq_client_instance::RouteRefreshConcurrentProbe;
#[doc(hidden)]
pub use crate::factory::mq_client_instance::RouteRefreshShardProbe;
pub use crate::hook::consume_message_context::ConsumeMessageContext;
pub use crate::hook::consume_message_hook::ConsumeMessageHook;
pub use crate::hook::consume_message_hook::ConsumeMessageHookArc;
#[doc(hidden)]
pub use crate::implementation::mq_client_api_factory::run_namesrv_refresh_lifecycle_probe;
pub use crate::implementation::mq_client_api_factory::MQClientAPIFactory;
#[doc(hidden)]
pub use crate::implementation::mq_client_api_factory::NamesrvRefreshLifecycleProbe;
#[doc(hidden)]
pub use crate::runtime::client_runtime_fallback_snapshot;
#[doc(hidden)]
pub use crate::runtime::reset_client_runtime_fallback_for_diagnostics;
#[doc(hidden)]
pub use crate::runtime::spawn_client_runtime_probe_task;
#[doc(hidden)]
pub use crate::runtime::ClientRuntimeTaskHandle;
#[doc(hidden)]
pub use crate::runtime::ClientSharedFallbackLifecycleState;
#[doc(hidden)]
pub use crate::runtime::ClientSharedFallbackSnapshot;
#[doc(hidden)]
pub use crate::stat::consumer_stats_manager::run_consumer_stats_manager_lifecycle_probe;
#[doc(hidden)]
pub use crate::stat::consumer_stats_manager::ConsumerStatsManagerLifecycleProbe;
pub type MQClientAPIExt = crate::implementation::mq_client_api_impl::MQClientAPIImpl;
pub type MqClientAdminImpl = crate::implementation::mq_client_api_impl::MQClientAPIImpl;
#[doc(hidden)]
pub use crate::latency::latency_fault_tolerance_impl::run_latency_fault_detector_lifecycle_probe;
#[doc(hidden)]
pub use crate::latency::latency_fault_tolerance_impl::LatencyFaultDetectorLifecycleProbe;
pub use crate::latency::BrokerFilter;
pub use crate::latency::MQFaultStrategy;
pub use crate::latency::Resolver;
pub use crate::latency::ServiceDetector;
#[allow(deprecated)]
pub use crate::legacy::ConsumeMessageOpenTracingHookImpl;
pub use crate::legacy::ConsumeRequest;
#[allow(deprecated)]
pub use crate::legacy::DefaultMQPullConsumer;
#[allow(deprecated)]
pub use crate::legacy::DefaultMQPullConsumerImpl;
pub use crate::legacy::DoNothingClientRemotingProcessor;
#[allow(deprecated)]
pub use crate::legacy::EndTransactionOpenTracingHookImpl;
#[allow(deprecated)]
pub use crate::legacy::MQHelper;
#[allow(deprecated)]
pub use crate::legacy::MQPullConsumer;
#[allow(deprecated)]
pub use crate::legacy::MQPullConsumerScheduleService;
#[allow(deprecated)]
pub use crate::legacy::PullTaskCallback;
#[allow(deprecated)]
pub use crate::legacy::PullTaskContext;
#[allow(deprecated)]
pub use crate::legacy::PullTaskImpl;
pub use crate::legacy::RebalanceImpl;
#[allow(deprecated)]
pub use crate::legacy::RebalancePullImpl;
#[allow(deprecated)]
pub use crate::legacy::SendMessageOpenTracingHookImpl;
#[allow(deprecated)]
pub use crate::legacy::TransactionCheckListener;
pub use crate::lock::ReadWriteCASLock;
#[doc(hidden)]
pub use crate::producer::produce_accumulator::run_produce_accumulator_guard_lifecycle_probe;
#[doc(hidden)]
pub use crate::producer::produce_accumulator::ProduceAccumulatorGuardLifecycleProbe;
#[doc(hidden)]
pub use crate::producer::request_future_holder::run_request_future_holder_lifecycle_probe;
#[doc(hidden)]
pub use crate::producer::request_future_holder::run_request_future_holder_scan_probe;
#[doc(hidden)]
pub use crate::producer::request_future_holder::RequestFutureHolderLifecycleProbe;
#[doc(hidden)]
pub use crate::producer::request_future_holder::RequestFutureHolderScanProbe;
pub use crate::producer::DefaultMQProducer;
pub use crate::producer::JavaHashCode;
pub use crate::producer::LocalTransactionState;
pub use crate::producer::MQProducer;
pub use crate::producer::MessageQueueSelector;
pub use crate::producer::MessageQueueSelectorFn;
pub use crate::producer::RequestCallback;
pub use crate::producer::SelectMessageQueueByHash;
pub use crate::producer::SelectMessageQueueByMachineRoom;
pub use crate::producer::SelectMessageQueueByRandom;
pub use crate::producer::SendCallback;
pub use crate::producer::SendResult;
pub use crate::producer::SendStatus;
pub use crate::producer::TransactionListener;
pub use crate::producer::TransactionMQProducer;
pub use crate::producer::TransactionMQProducerBuilder;
pub use crate::producer::TransactionSendResult;
#[doc(hidden)]
pub use crate::trace::async_trace_dispatcher::run_trace_queue_depth_accounting_probe;
#[doc(hidden)]
pub use crate::trace::async_trace_dispatcher::run_trace_worker_lifecycle_probe;
pub use crate::trace::async_trace_dispatcher::AsyncTraceDispatcher;
#[doc(hidden)]
pub use crate::trace::async_trace_dispatcher::TraceWorkerLifecycleProbe;
pub use crate::trace::trace_data_encoder::TraceDataEncoder;
pub use crate::trace::trace_dispatcher::ArcTraceDispatcher;
pub use crate::trace::trace_dispatcher::TraceDispatcher;
pub use crate::trace::trace_dispatcher::Type as TraceDispatcherOperation;
pub use crate::trace::trace_dispatcher_type::TraceDispatcherType;
pub use crate::trace::trace_type::TraceType;

#[cfg(test)]
mod tests {
    #[test]
    fn mq_client_err_without_response_code_preserves_message() {
        let err = crate::mq_client_err!("simple client error");

        match err {
            rocketmq_error::RocketMQError::IllegalArgument(message) => {
                assert!(message.contains("simple client error"));
                assert!(!message.contains("Body is empty"));
            }
            other => panic!("expected illegal argument error, got {other:?}"),
        }
    }
}
