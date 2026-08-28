// Copyright 2026 The RocketMQ Rust Authors
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

use std::sync::Arc;

use crossbeam_skiplist::SkipSet;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::ResourceBudget;
use rocketmq_runtime::ResourceBudgetTree;
use rocketmq_store::MessageStoreConfig;
use rocketmq_transport::api::v1::Channel;
use rocketmq_transport::api::v1::ConnectionHandlerContext;
use rocketmq_transport::api::v1::ConnectionHandlerContextWrapper;
use rocketmq_transport::test_support::Connection;

use super::PopLongPollingPolicy;
use super::PopLongPollingRequestProcessor;
use super::PopLongPollingService;
use super::PopLongPollingServiceContext;
use crate::broker_runtime::BrokerRuntime;
use crate::config::broker_config::BrokerConfig;
use crate::long_polling::pop_request::PopRequest;

struct TestProcessor;

impl PopLongPollingRequestProcessor for TestProcessor {
    async fn process_request_when_wakeup(
        &self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request: RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        Ok(None)
    }
}

fn test_service(processor: &Arc<TestProcessor>) -> PopLongPollingService<TestProcessor> {
    let mut runtime = BrokerRuntime::new(
        Arc::new(BrokerConfig::default()),
        Arc::new(MessageStoreConfig::default()),
    );
    let state = runtime.runtime_state_mut();
    let context = PopLongPollingServiceContext::new(
        PopLongPollingPolicy::from_config(&state.broker_config()),
        state.topic_config_manager_handle(),
        state.subscription_group_manager().config_lookup(),
        state.broker_service_context(),
    );
    PopLongPollingService::new(context, false, Arc::downgrade(processor))
}

fn request_budget() -> ResourceBudget {
    ResourceBudgetTree::new(
        "pop-long-pinned-node",
        BudgetLimit::new(1, 64 * 1024, FullPolicy::Reject),
    )
    .expect("request budget")
    .root()
}

async fn test_context() -> ConnectionHandlerContext {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind local test listener");
    let local_addr = listener.local_addr().expect("local listener address");
    let stream = std::net::TcpStream::connect(local_addr).expect("connect local test listener");
    stream.set_nonblocking(true).expect("set nonblocking");
    let stream = tokio::net::TcpStream::from_std(stream).expect("convert TCP stream");
    let channel = rocketmq_transport::test_support::TestChannelBuilder::new(
        Connection::new(stream),
        crate::test_task_group("pop-long-pinned-node-channel"),
    )
    .addresses(local_addr, local_addr)
    .build()
    .expect("build test channel");
    Arc::new(ConnectionHandlerContextWrapper::new(channel))
}

async fn budgeted_request(budget: &ResourceBudget, expired: u64) -> (Arc<PopRequest>, usize) {
    let command = RemotingCommand::create_remoting_command(0);
    let retained_bytes = PopRequest::estimated_retained_bytes(&command);
    let permit = budget
        .try_acquire_data(retained_bytes)
        .expect("first suspended request should fit");
    (
        Arc::new(PopRequest::new_with_resource_permit(
            command,
            test_context().await,
            expired,
            None,
            None,
            permit,
        )),
        retained_bytes,
    )
}

fn assert_released_and_readmits(budget: &ResourceBudget, retained_bytes: usize) {
    let terminal = budget.snapshot();
    assert_eq!(terminal.current_count, 0);
    assert_eq!(terminal.current_bytes, 0);

    let readmitted = budget
        .try_acquire_data(retained_bytes)
        .expect("logical terminal must return capacity while the removed node remains pinned");
    assert_eq!(budget.snapshot().current_count, 1);
    assert_eq!(budget.snapshot().current_bytes, retained_bytes);
    drop(readmitted);
    assert_eq!(budget.snapshot().current_count, 0);
    assert_eq!(budget.snapshot().current_bytes, 0);
}

#[tokio::test]
async fn arrival_and_duplicate_terminals_release_shared_permit_while_node_stays_pinned() {
    let processor = Arc::new(TestProcessor);
    let service = test_service(&processor);
    let budget = request_budget();
    let (request, retained_bytes) = budgeted_request(&budget, current_millis().saturating_add(30_000)).await;
    let queue = SkipSet::new();
    queue.insert(request);
    service.total_polling_num.store(1, std::sync::atomic::Ordering::Release);
    let pinned_node = queue.front().expect("suspended request node");

    let request = service
        .poll_remoting_commands(&queue)
        .expect("arrival claims suspended request");
    let duplicate = Arc::clone(&request);
    assert!(!service.wake_up(request));
    assert!(!service.wake_up(duplicate));

    assert_released_and_readmits(&budget, retained_bytes);
    assert!(pinned_node.is_removed(), "entry guard must still pin the removed node");
}

#[tokio::test]
async fn timeout_terminal_releases_shared_permit_while_node_stays_pinned() {
    let processor = Arc::new(TestProcessor);
    let service = test_service(&processor);
    let budget = request_budget();
    let (request, retained_bytes) = budgeted_request(&budget, 0).await;
    let queue = SkipSet::new();
    queue.insert(request);
    service.total_polling_num.store(1, std::sync::atomic::Ordering::Release);
    let pinned_node = queue.front().expect("suspended request node");

    service.wake_up_expired_requests(&queue);

    assert_released_and_readmits(&budget, retained_bytes);
    assert!(pinned_node.is_removed(), "entry guard must still pin the removed node");
}

#[tokio::test]
async fn cancellation_drain_releases_shared_permit_while_node_stays_pinned() {
    let processor = Arc::new(TestProcessor);
    let service = test_service(&processor);
    let budget = request_budget();
    let (request, retained_bytes) = budgeted_request(&budget, current_millis().saturating_add(30_000)).await;
    let queue = SkipSet::new();
    queue.insert(request);
    service.total_polling_num.store(1, std::sync::atomic::Ordering::Release);
    let pinned_node = queue.front().expect("suspended request node");

    service.drain_polling_queue(&queue);

    assert_released_and_readmits(&budget, retained_bytes);
    assert!(pinned_node.is_removed(), "entry guard must still pin the removed node");
}

#[tokio::test]
async fn inactive_terminal_releases_shared_permit_while_node_stays_pinned() {
    let processor = Arc::new(TestProcessor);
    let service = test_service(&processor);
    let budget = request_budget();
    let (request, retained_bytes) = budgeted_request(&budget, current_millis().saturating_add(30_000)).await;
    request.get_channel().connection_ref().close();
    let queue = SkipSet::new();
    queue.insert(request);
    service.total_polling_num.store(1, std::sync::atomic::Ordering::Release);
    let pinned_node = queue.front().expect("suspended request node");

    assert!(service.poll_remoting_commands(&queue).is_none());

    assert_released_and_readmits(&budget, retained_bytes);
    assert!(pinned_node.is_removed(), "entry guard must still pin the removed node");
}

#[tokio::test]
async fn resource_removal_releases_shared_permit_while_node_stays_pinned() {
    let processor = Arc::new(TestProcessor);
    let service = test_service(&processor);
    let budget = request_budget();
    let (request, retained_bytes) = budgeted_request(&budget, current_millis().saturating_add(30_000)).await;
    let queue = SkipSet::new();
    queue.insert(request);
    service.total_polling_num.store(1, std::sync::atomic::Ordering::Release);
    let pinned_node = queue.front().expect("suspended request node");

    service.discard_polling_queue(&queue);

    assert_released_and_readmits(&budget, retained_bytes);
    assert!(pinned_node.is_removed(), "entry guard must still pin the removed node");
}
