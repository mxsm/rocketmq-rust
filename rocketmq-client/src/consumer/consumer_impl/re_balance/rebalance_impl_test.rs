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
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_model::allocation::AllocateMessageQueueAveragely;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_model::common::mix_all;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::body::query_assignment_response_body::QueryAssignmentResponseBody;
use rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
use rocketmq_protocol::protocol::route::route_data_view::BrokerData;
use rocketmq_protocol::protocol::route::route_data_view::QueueData;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_protocol::protocol::RemotingSerializable;
use rocketmq_protocol::RemotingCommand;
use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_transport::api::AdmissionController;
use rocketmq_transport::api::AdmissionLimits;
use rocketmq_transport::test_support::SessionProcessor;
use rocketmq_transport::test_support::SessionTransportServer;
use rocketmq_transport::test_support::SessionTransportServerConfig;
use tokio::sync::Notify;
use tokio::time::timeout;

use super::RebalanceImpl;
use crate::base::client_config::ClientConfig;
use crate::consumer::consumer_impl::pop_process_queue::PopProcessQueue;
use crate::consumer::consumer_impl::pop_request::PopRequest;
use crate::consumer::consumer_impl::process_queue::ProcessQueue;
use crate::consumer::consumer_impl::pull_request::PullRequest;
use crate::consumer::consumer_impl::re_balance::Rebalance;
use crate::factory::mq_client_instance::MQClientInstance;

struct AssignmentProcessor {
    broker_addr: std::sync::OnceLock<CheetahString>,
    assignment_responses: Mutex<VecDeque<RemotingCommand>>,
    assignment_requests: std::sync::atomic::AtomicUsize,
    route_requests: std::sync::atomic::AtomicUsize,
}

impl AssignmentProcessor {
    fn new() -> Self {
        Self {
            broker_addr: std::sync::OnceLock::new(),
            assignment_responses: Mutex::new(VecDeque::new()),
            assignment_requests: std::sync::atomic::AtomicUsize::new(0),
            route_requests: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn route(&self) -> rocketmq_error::RocketMQResult<TopicRouteData> {
        let broker_addr = self
            .broker_addr
            .get()
            .ok_or_else(|| rocketmq_error::RocketMQError::invariant_violated("test broker address is unset"))?;
        Ok(TopicRouteData {
            queue_datas: vec![QueueData::new(
                CheetahString::from_static_str("broker-a"),
                1,
                1,
                rocketmq_model::common::constant::PermName::PERM_READ
                    | rocketmq_model::common::constant::PermName::PERM_WRITE,
                0,
            )],
            broker_datas: vec![BrokerData::new(
                CheetahString::from_static_str("cluster-a"),
                CheetahString::from_static_str("broker-a"),
                [(mix_all::MASTER_ID, broker_addr.clone())].into_iter().collect(),
                None,
            )],
            ..Default::default()
        })
    }

    fn push_assignment(&self, response: RemotingCommand) {
        self.assignment_responses
            .lock()
            .expect("assignment response queue")
            .push_back(response);
    }

    fn counts(&self) -> (usize, usize) {
        use std::sync::atomic::Ordering;

        (
            self.assignment_requests.load(Ordering::SeqCst),
            self.route_requests.load(Ordering::SeqCst),
        )
    }
}

impl SessionProcessor for AssignmentProcessor {
    fn process(
        &self,
        request: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = rocketmq_error::RocketMQResult<RemotingCommand>> + Send + '_>> {
        Box::pin(async move {
            use std::sync::atomic::Ordering;

            let response = if request.code() == RequestCode::QueryAssignment.to_i32() {
                self.assignment_requests.fetch_add(1, Ordering::SeqCst);
                self.assignment_responses
                    .lock()
                    .expect("assignment response queue")
                    .pop_front()
                    .ok_or_else(|| rocketmq_error::RocketMQError::illegal_argument("unexpected assignment request"))?
            } else if request.code() == RequestCode::GetRouteinfoByTopic.to_i32() {
                self.route_requests.fetch_add(1, Ordering::SeqCst);
                RemotingCommand::create_success_response_command().set_body(self.route()?.encode()?)
            } else {
                return Err(rocketmq_error::RocketMQError::illegal_argument(format!(
                    "unexpected request code {}",
                    request.code()
                )));
            };
            Ok(response.set_opaque(request.opaque()))
        })
    }
}

struct BlockingRemovalRebalance {
    callback_started: Notify,
    release_callback: Notify,
    offset_started: Notify,
    release_offset: Notify,
}

impl Rebalance for BlockingRemovalRebalance {
    async fn message_queue_changed(
        &self,
        _topic: &str,
        _mq_all: &HashSet<MessageQueue>,
        _mq_divided: &HashSet<MessageQueue>,
    ) {
    }

    async fn remove_unnecessary_message_queue(&self, _mq: &MessageQueue, _pq: &ProcessQueue) -> bool {
        self.callback_started.notify_one();
        self.release_callback.notified().await;
        true
    }

    fn consume_type(&self) -> ConsumeType {
        ConsumeType::ConsumePassively
    }

    async fn remove_dirty_offset(&self, _mq: &MessageQueue) {
        self.offset_started.notify_one();
        self.release_offset.notified().await;
    }

    async fn compute_pull_from_where_with_exception(&self, _mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        Ok(0)
    }

    async fn compute_pull_from_where(&self, _mq: &MessageQueue) -> i64 {
        0
    }

    fn get_consume_init_mode(&self) -> i32 {
        0
    }

    async fn dispatch_pull_request(&self, _pull_request_list: Vec<PullRequest>, _delay: u64) {}

    async fn dispatch_pop_pull_request(&self, _pop_request_list: Vec<PopRequest>, _delay: u64) {}

    fn create_process_queue(&self) -> ProcessQueue {
        ProcessQueue::new()
    }

    fn create_pop_process_queue(&self) -> PopProcessQueue {
        PopProcessQueue::new()
    }

    async fn remove_process_queue(&self, _mq: &MessageQueue) {}

    async fn unlock(&self, _mq: &MessageQueue, _oneway: bool) {}

    async fn lock_all(&self) {}

    async fn unlock_all(&self, _oneway: bool) {}

    async fn do_rebalance(&self, _is_order: bool) -> bool {
        true
    }

    fn client_rebalance(&self, _topic: &str) -> bool {
        true
    }

    fn destroy(&self) {}
}

#[tokio::test]
async fn assignment_retry_refreshes_route_once_and_reuses_the_operation_budget() {
    let server_runtime = RuntimeContext::from_current("assignment-retry-test");
    let processor = Arc::new(AssignmentProcessor::new());
    let server = SessionTransportServer::bind(
        server_runtime.service_context("assignment-server"),
        SessionTransportServerConfig::loopback(),
        Arc::clone(&processor) as Arc<dyn SessionProcessor>,
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    )
    .await
    .expect("bind assignment server");
    let broker_addr = CheetahString::from_string(server.local_addr().to_string());
    processor
        .broker_addr
        .set(broker_addr.clone())
        .expect("set assignment broker address once");
    server.start().expect("start assignment server");
    processor.push_assignment(RemotingCommand::create_response_command_with_code_remark(
        ResponseCode::SystemError,
        "refresh-route",
    ));
    processor.push_assignment(
        RemotingCommand::create_success_response_command().set_body(
            QueryAssignmentResponseBody::default()
                .encode()
                .expect("assignment body"),
        ),
    );

    let client_runtime = crate::runtime::test_client_runtime("assignment-retry-test");
    let mut client_config = ClientConfig::default();
    client_config.set_vip_channel_enabled(false);
    let client_instance = MQClientInstance::new_arc(
        client_config,
        0,
        "assignment-retry-client",
        None,
        client_runtime.component("instance"),
        client_runtime.telemetry_handle().clone(),
        client_runtime.pool().request_future_holder(),
    );
    let api = client_instance.get_mq_client_api_impl().expect("client API");
    api.update_name_server_address_list_sync(broker_addr.as_str());
    api.start().await.expect("start assignment client transport");
    let topic = CheetahString::from_static_str("assignment-topic");
    client_instance
        .topic_route_table
        .insert(topic.clone(), processor.route().expect("initial assignment route"));
    let rebalance = RebalanceImpl::<BlockingRemovalRebalance>::new(
        Some(CheetahString::from_static_str("assignment-group")),
        Some(MessageModel::Clustering),
        Some(Arc::new(AllocateMessageQueueAveragely)),
        Some(Arc::clone(&client_instance)),
    );

    assert!(rebalance.try_query_assignment(&topic).await);
    assert_eq!(processor.counts(), (2, 1));
    assert!(rebalance.topic_broker_rebalance.contains_key(&topic));
    assert!(!rebalance.topic_client_rebalance.contains_key(&topic));

    client_instance.shutdown().await;
    client_runtime
        .shutdown()
        .await
        .assert_no_task_leak()
        .expect("client runtime tasks drained");
    server
        .shutdown_until(ShutdownDeadline::after(Duration::from_secs(5)))
        .await
        .assert_no_task_leak()
        .expect("assignment server tasks drained");
    server_runtime
        .shutdown_tasks(Duration::from_secs(5))
        .await
        .assert_no_task_leak()
        .expect("assignment runtime tasks drained");
}

#[tokio::test]
async fn blocked_removal_callback_does_not_hold_queue_table_or_remove_replacement() {
    let topic = CheetahString::from_static_str("topic-a");
    let mq = MessageQueue::from_parts(topic.clone(), "broker-a", 0);
    let original = Arc::new(ProcessQueue::new());
    let replacement = Arc::new(ProcessQueue::new());
    let callback = Arc::new(BlockingRemovalRebalance {
        callback_started: Notify::new(),
        release_callback: Notify::new(),
        offset_started: Notify::new(),
        release_offset: Notify::new(),
    });
    let rebalance = Arc::new(RebalanceImpl::new(
        Some(CheetahString::from_static_str("group-a")),
        None,
        None,
        None,
    ));
    let callback_set = rebalance.sub_rebalance_impl.set(Arc::downgrade(&callback));
    assert!(callback_set.is_ok(), "test callback should be initialized once");
    rebalance.process_queue_table.write().await.insert(mq.clone(), original);

    let removal = tokio::spawn({
        let rebalance = rebalance.clone();
        let topic = topic.clone();
        async move {
            rebalance
                .update_process_queue_table_in_rebalance(&topic, &HashSet::new(), false)
                .await
        }
    });

    let callback_started = timeout(Duration::from_secs(1), callback.callback_started.notified()).await;
    assert!(callback_started.is_ok(), "removal callback should start");
    let replacement_inserted = timeout(Duration::from_millis(100), async {
        rebalance
            .process_queue_table
            .write()
            .await
            .insert(mq.clone(), replacement.clone());
    })
    .await
    .is_ok();

    callback.release_callback.notify_one();
    let removal_result = timeout(Duration::from_secs(1), removal).await;
    assert!(
        matches!(&removal_result, Ok(Ok(_))),
        "rebalance should finish after releasing the callback without panicking"
    );
    let Ok(Ok(changed)) = removal_result else {
        return;
    };

    assert!(
        replacement_inserted,
        "a blocked removal callback must not retain the process-queue table lock"
    );
    let current = rebalance.process_queue_table.read().await.get(&mq).cloned();
    assert!(current.is_some(), "replacement queue must survive the stale callback");
    let Some(current) = current else {
        return;
    };
    assert!(Arc::ptr_eq(&current, &replacement));
    assert!(!changed, "a stale callback must not report removal of a replacement");
}

#[tokio::test]
async fn blocked_offset_lookup_does_not_hold_queue_table_or_overwrite_concurrent_insert() {
    let topic = CheetahString::from_static_str("topic-a");
    let mq = MessageQueue::from_parts(topic.clone(), "broker-a", 0);
    let replacement = Arc::new(ProcessQueue::new());
    let callback = Arc::new(BlockingRemovalRebalance {
        callback_started: Notify::new(),
        release_callback: Notify::new(),
        offset_started: Notify::new(),
        release_offset: Notify::new(),
    });
    let rebalance = Arc::new(RebalanceImpl::new(
        Some(CheetahString::from_static_str("group-a")),
        None,
        None,
        None,
    ));
    let callback_set = rebalance.sub_rebalance_impl.set(Arc::downgrade(&callback));
    assert!(callback_set.is_ok(), "test callback should be initialized once");

    let update = tokio::spawn({
        let rebalance = rebalance.clone();
        let topic = topic.clone();
        let mq = mq.clone();
        async move {
            rebalance
                .update_process_queue_table_in_rebalance(&topic, &HashSet::from([mq]), false)
                .await
        }
    });

    let offset_started = timeout(Duration::from_secs(1), callback.offset_started.notified()).await;
    assert!(offset_started.is_ok(), "offset lookup should start");
    let replacement_inserted = timeout(Duration::from_millis(100), async {
        rebalance
            .process_queue_table
            .write()
            .await
            .insert(mq.clone(), replacement.clone());
    })
    .await
    .is_ok();

    callback.release_offset.notify_one();
    let update_result = timeout(Duration::from_secs(1), update).await;
    assert!(
        matches!(&update_result, Ok(Ok(_))),
        "rebalance should finish after releasing the offset lookup without panicking"
    );
    let Ok(Ok(changed)) = update_result else {
        return;
    };

    assert!(
        replacement_inserted,
        "a blocked offset lookup must not retain the process-queue table lock"
    );
    let current = rebalance.process_queue_table.read().await.get(&mq).cloned();
    assert!(current.is_some(), "concurrently inserted queue must remain present");
    let Some(current) = current else {
        return;
    };
    assert!(Arc::ptr_eq(&current, &replacement));
    assert!(!changed, "a concurrent insertion must win the conditional commit");
}
