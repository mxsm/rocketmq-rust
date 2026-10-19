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

//! Read-client implementation of the address-free consumer client runtime read.

use cheetah_string::CheetahString;
use rocketmq_client_rust::DefaultMQAdminExt;
use rocketmq_client_rust::MQAdminConsumerRuntimeReadExt;
use rocketmq_client_rust::MQAdminReadExt;
use rocketmq_error::Error as CanonicalError;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_protocol::protocol::body::consumer_running_info::ConsumerRunningInfo;

use crate::core::consumer_runtime::ConsumerClientQueueMode;
use crate::core::consumer_runtime::ConsumerClientQueueRuntime;
use crate::core::consumer_runtime::ConsumerClientRuntime;
use crate::core::consumer_runtime::ConsumerClientSubscription;
use crate::core::consumer_runtime::ConsumerClientTopicStatus;
use crate::core::consumer_runtime::QueryConsumerClientRuntimeRequest;
use crate::core::consumer_runtime::MAX_CONSUMER_CLIENT_EXPRESSION_BYTES;
use crate::core::consumer_runtime::MAX_CONSUMER_CLIENT_QUEUES;
use crate::core::consumer_runtime::MAX_CONSUMER_CLIENT_TOPICS;
use crate::core::query::AdminQuerySource;
use crate::core::AdminError;
use crate::core::AdminResult;
use crate::exact_broker::resolve_exact_broker_targets;
use crate::topic_observation::backend_error;

const OPERATION: &str = "query_consumer_client_runtime";
const MAX_NAME_BYTES: usize = 255;
const MAX_LABEL_BYTES: usize = 64;
/// The expression type of a client that reports none: every client older than typed filters.
const TAG_EXPRESSION: &str = "TAG";

#[allow(async_fn_in_trait)]
trait ConsumerRuntimeSource: Send {
    async fn cluster_info(&self) -> Result<ClusterInfo, CanonicalError>;
    async fn running_info(
        &self,
        broker_addr: CheetahString,
        consumer_group: &str,
        client_id: &str,
    ) -> Result<ConsumerRunningInfo, CanonicalError>;
}

impl ConsumerRuntimeSource for DefaultMQAdminExt {
    async fn cluster_info(&self) -> Result<ClusterInfo, CanonicalError> {
        MQAdminReadExt::examine_broker_cluster_info(self)
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)
    }

    async fn running_info(
        &self,
        broker_addr: CheetahString,
        consumer_group: &str,
        client_id: &str,
    ) -> Result<ConsumerRunningInfo, CanonicalError> {
        MQAdminConsumerRuntimeReadExt::consumer_running_info_at(
            self,
            broker_addr,
            CheetahString::from(consumer_group),
            CheetahString::from(client_id),
        )
        .await
        .map_err(crate::IntoCanonicalError::into_canonical_error)
    }
}

pub(crate) async fn query_consumer_client_runtime(
    admin: &DefaultMQAdminExt,
    request: &QueryConsumerClientRuntimeRequest,
) -> AdminResult<ConsumerClientRuntime> {
    query_consumer_client_runtime_from(admin, request).await
}

/// Asks the instances of the named Broker in ascending order of their identifier, the master
/// first, and returns what the first one relays. A client is connected to every instance it
/// reads from, so an instance that does not know the client is not the last word.
async fn query_consumer_client_runtime_from<S: ConsumerRuntimeSource>(
    source: &S,
    request: &QueryConsumerClientRuntimeRequest,
) -> AdminResult<ConsumerClientRuntime> {
    let request = QueryConsumerClientRuntimeRequest::try_new(
        &request.cluster,
        &request.consumer_group,
        &request.broker_name,
        &request.client_id,
    )?;
    let cluster_info = source
        .cluster_info()
        .await
        .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
    let (targets, _) = resolve_exact_broker_targets(
        cluster_info,
        &request.cluster,
        &request.broker_name,
        AdminQuerySource::ConsumerConnection,
    )?;

    let mut last_error = None;
    for (_, broker_addr) in targets {
        match source
            .running_info(broker_addr, &request.consumer_group, &request.client_id)
            .await
        {
            Ok(info) => return project(info),
            Err(error) => last_error = Some(error),
        }
    }
    // Whatever a Broker called its refusal, a relay that failed is a failed source: the caller
    // named a client from a list this adapter was given, not an argument of its own.
    Err(match last_error {
        Some(error) => AdminError::operation_failed_with_source("get_consumer_running_info", error),
        None => AdminError::unavailable(OPERATION, "No instance of the Broker could be asked"),
    })
}

fn project(info: ConsumerRunningInfo) -> AdminResult<ConsumerClientRuntime> {
    let property = |name: &str| info.properties.get(name).map(String::as_str);
    let mut truncated = false;

    let mut subscriptions = Vec::new();
    for subscription in &info.subscription_set {
        if subscriptions.len() == MAX_CONSUMER_CLIENT_TOPICS {
            truncated = true;
            break;
        }
        let expression = subscription.sub_string.as_str();
        subscriptions.push(ConsumerClientSubscription {
            topic: name(subscription.topic.as_str())?,
            expression_type: label(subscription.expression_type.as_str()).unwrap_or_else(|| TAG_EXPRESSION.to_owned()),
            expression: (expression.len() <= MAX_CONSUMER_CLIENT_EXPRESSION_BYTES && !has_control(expression))
                .then(|| expression.to_owned()),
            sub_version: subscription.sub_version,
        });
    }
    subscriptions.sort_by(|left, right| {
        (&left.topic, &left.expression_type, &left.expression).cmp(&(
            &right.topic,
            &right.expression_type,
            &right.expression,
        ))
    });

    let mut topic_status = Vec::new();
    for (topic, status) in &info.status_table {
        if topic_status.len() == MAX_CONSUMER_CLIENT_TOPICS {
            truncated = true;
            break;
        }
        topic_status.push(ConsumerClientTopicStatus {
            topic: name(topic)?,
            pull_rt: finite(status.pull_rt),
            pull_tps: finite(status.pull_tps),
            consume_rt: finite(status.consume_rt),
            consume_ok_tps: finite(status.consume_ok_tps),
            consume_failed_tps: finite(status.consume_failed_tps),
            consume_failed_msgs: status.consume_failed_msgs,
        });
    }

    let queue_count = info.mq_table.len().saturating_add(info.mq_pop_table.len());
    let mut queues = Vec::with_capacity(queue_count.min(MAX_CONSUMER_CLIENT_QUEUES));
    for (queue, held) in &info.mq_table {
        queues.push(ConsumerClientQueueRuntime {
            topic: name(queue.topic_str())?,
            broker_name: name(queue.broker_name().as_str())?,
            queue_id: queue.queue_id(),
            mode: ConsumerClientQueueMode::Pull,
            dropped: held.droped,
            last_pull_timestamp: timestamp(held.last_pull_timestamp),
            commit_offset: Some(held.commit_offset),
            cached_message_count: Some(held.cached_msg_count),
            cached_message_size_mib: Some(held.cached_msg_size_in_mib),
            locked: Some(held.locked),
            try_unlock_times: Some(held.try_unlock_times),
            last_lock_timestamp: timestamp(held.last_lock_timestamp),
            last_consume_timestamp: timestamp(held.last_consume_timestamp),
            wait_ack_count: None,
        });
    }
    for (queue, held) in &info.mq_pop_table {
        queues.push(ConsumerClientQueueRuntime {
            topic: name(queue.topic_str())?,
            broker_name: name(queue.broker_name().as_str())?,
            queue_id: queue.queue_id(),
            mode: ConsumerClientQueueMode::Pop,
            dropped: held.droped(),
            last_pull_timestamp: timestamp(held.last_pop_timestamp()),
            commit_offset: None,
            cached_message_count: None,
            cached_message_size_mib: None,
            locked: None,
            try_unlock_times: None,
            last_lock_timestamp: None,
            last_consume_timestamp: None,
            wait_ack_count: Some(held.wait_ack_count()),
        });
    }
    queues.sort_by(|left, right| {
        (&left.topic, &left.broker_name, left.queue_id, left.mode as u8).cmp(&(
            &right.topic,
            &right.broker_name,
            right.queue_id,
            right.mode as u8,
        ))
    });
    if queues.len() > MAX_CONSUMER_CLIENT_QUEUES {
        queues.truncate(MAX_CONSUMER_CLIENT_QUEUES);
        truncated = true;
    }

    Ok(ConsumerClientRuntime {
        consume_type: property(ConsumerRunningInfo::PROP_CONSUME_TYPE).and_then(label),
        consume_orderly: property(ConsumerRunningInfo::PROP_CONSUME_ORDERLY).and_then(|value| value.parse().ok()),
        thread_pool_core_size: property(ConsumerRunningInfo::PROP_THREADPOOL_CORE_SIZE)
            .and_then(|value| value.parse().ok()),
        client_version: property(ConsumerRunningInfo::PROP_CLIENT_VERSION).and_then(label),
        started_timestamp: property(ConsumerRunningInfo::PROP_CONSUMER_START_TIMESTAMP)
            .and_then(|value| value.parse::<u64>().ok())
            .and_then(timestamp),
        subscriptions,
        topic_status,
        queues,
        queue_count,
        truncated,
    })
}

/// Accepts a Topic or Broker name a client reported. A name no Broker would have accepted means
/// the answer is not what it claims to be, so the read fails instead of passing it on.
fn name(value: &str) -> AdminResult<String> {
    if value.is_empty() || value.len() > MAX_NAME_BYTES || has_control(value) {
        return Err(AdminError::operation_failed(
            OPERATION,
            "Consumer client reported an invalid name",
        ));
    }
    Ok(value.to_owned())
}

/// Keeps a short closed-vocabulary value, such as a version or a consume type, and nothing else.
fn label(value: &str) -> Option<String> {
    (!value.is_empty()
        && value.len() <= MAX_LABEL_BYTES
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.')))
    .then(|| value.to_owned())
}

fn has_control(value: &str) -> bool {
    value.chars().any(char::is_control)
}

fn finite(value: f64) -> f64 {
    if value.is_finite() {
        value
    } else {
        0.0
    }
}

/// Reads a client time in Unix milliseconds; a client reports zero for a time it never recorded.
fn timestamp(value: u64) -> Option<i64> {
    i64::try_from(value).ok().filter(|value| *value > 0)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::sync::Mutex;

    use rocketmq_model::common::mix_all;
    use rocketmq_model::message::MessageQueue;
    use rocketmq_protocol::protocol::body::consume_status::ConsumeStatus;
    use rocketmq_protocol::protocol::body::pop_process_queue_info::PopProcessQueueInfo;
    use rocketmq_protocol::protocol::body::process_queue_info::ProcessQueueInfo;
    use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
    use rocketmq_protocol::protocol::route::route_data_view::BrokerData;

    use super::*;
    use crate::core::AdminFailure;

    const MASTER: &str = "127.0.0.1:10911";
    const SLAVE: &str = "127.0.0.2:10911";

    struct FakeSource {
        cluster_info: ClusterInfo,
        /// The addresses that relay an answer; every other address fails.
        answering: Vec<&'static str>,
        info: ConsumerRunningInfo,
        calls: Mutex<Vec<(String, String, String)>>,
    }

    impl ConsumerRuntimeSource for FakeSource {
        async fn cluster_info(&self) -> Result<ClusterInfo, CanonicalError> {
            Ok(self.cluster_info.clone())
        }

        async fn running_info(
            &self,
            broker_addr: CheetahString,
            consumer_group: &str,
            client_id: &str,
        ) -> Result<ConsumerRunningInfo, CanonicalError> {
            self.calls
                .lock()
                .unwrap()
                .push((broker_addr.to_string(), consumer_group.to_owned(), client_id.to_owned()));
            if self.answering.contains(&broker_addr.as_str()) {
                Ok(self.info.clone())
            } else {
                Err(
                    CanonicalError::new(&rocketmq_error::PROTOCOL_RESPONSE_FAILED).with_context(
                        rocketmq_error::ErrorContext::new()
                            .with_text(rocketmq_error::fields::OPERATION_DIAGNOSTIC, "consumer_runtime_test")
                            .with_secret_presence(rocketmq_error::fields::REASON_PRESENT),
                    ),
                )
            }
        }
    }

    fn cluster_info() -> ClusterInfo {
        let broker = BrokerData::new(
            "cluster-a".into(),
            "broker-a".into(),
            HashMap::from([(mix_all::MASTER_ID, MASTER.into()), (1, SLAVE.into())]),
            None,
        );
        ClusterInfo::new(
            Some(HashMap::from([("broker-a".into(), broker)])),
            Some(HashMap::from([(
                CheetahString::from("cluster-a"),
                HashSet::from([CheetahString::from("broker-a")]),
            )])),
        )
    }

    fn source(answering: &[&'static str], info: ConsumerRunningInfo) -> FakeSource {
        FakeSource {
            cluster_info: cluster_info(),
            answering: answering.to_vec(),
            info,
            calls: Mutex::default(),
        }
    }

    fn request() -> QueryConsumerClientRuntimeRequest {
        QueryConsumerClientRuntimeRequest::try_new("cluster-a", "workers", "broker-a", "client-1").unwrap()
    }

    fn subscription(topic: &str, expression: &str) -> SubscriptionData {
        SubscriptionData {
            topic: topic.into(),
            sub_string: expression.into(),
            expression_type: "TAG".into(),
            sub_version: 7,
            ..Default::default()
        }
    }

    fn reported() -> ConsumerRunningInfo {
        let mut info = ConsumerRunningInfo::new();
        for (name, value) in [
            (ConsumerRunningInfo::PROP_NAMESERVER_ADDR, "10.0.0.9:9876"),
            (ConsumerRunningInfo::PROP_CONSUME_TYPE, "CONSUME_PASSIVELY"),
            (ConsumerRunningInfo::PROP_CONSUME_ORDERLY, "false"),
            (ConsumerRunningInfo::PROP_THREADPOOL_CORE_SIZE, "20"),
            (ConsumerRunningInfo::PROP_CLIENT_VERSION, "V5_3_0"),
            (ConsumerRunningInfo::PROP_CONSUMER_START_TIMESTAMP, "1700000000000"),
            ("accessKey", "private"),
        ] {
            info.properties.insert(name.to_owned(), value.to_owned());
        }
        info.subscription_set.insert(subscription("orders", "TagA || TagB"));
        info.subscription_set.insert(subscription("%RETRY%workers", "*"));
        info.status_table.insert(
            "orders".to_owned(),
            ConsumeStatus {
                pull_rt: 1.5,
                pull_tps: f64::NAN,
                consume_rt: 2.5,
                consume_ok_tps: 10.0,
                consume_failed_tps: 0.0,
                consume_failed_msgs: 3,
            },
        );
        info.mq_table.insert(
            MessageQueue::from_parts("orders", "broker-a", 1),
            ProcessQueueInfo {
                commit_offset: 40,
                cached_msg_count: 5,
                cached_msg_size_in_mib: 1,
                locked: true,
                last_pull_timestamp: 1_700_000_000_500,
                ..Default::default()
            },
        );
        info.mq_table.insert(
            MessageQueue::from_parts("orders", "broker-a", 0),
            ProcessQueueInfo {
                commit_offset: 30,
                droped: true,
                ..Default::default()
            },
        );
        info.mq_pop_table.insert(
            MessageQueue::from_parts("orders", "broker-a", 0),
            PopProcessQueueInfo::new(4, false, 1_700_000_000_900),
        );
        info.jstack = Some("thread dump".to_owned());
        info
    }

    #[tokio::test]
    async fn the_master_is_asked_first_and_the_next_instance_only_when_it_does_not_answer() {
        let master = source(&[MASTER, SLAVE], reported());
        query_consumer_client_runtime_from(&master, &request()).await.unwrap();
        assert_eq!(
            *master.calls.lock().unwrap(),
            [(MASTER.to_owned(), "workers".to_owned(), "client-1".to_owned())]
        );

        let slave = source(&[SLAVE], reported());
        query_consumer_client_runtime_from(&slave, &request()).await.unwrap();
        let asked = slave.calls.lock().unwrap();
        assert_eq!(
            asked.iter().map(|call| call.0.as_str()).collect::<Vec<_>>(),
            [MASTER, SLAVE]
        );
    }

    #[tokio::test]
    async fn only_the_named_members_of_what_a_client_reports_are_kept() {
        let source = source(&[MASTER], reported());

        let runtime = query_consumer_client_runtime_from(&source, &request()).await.unwrap();

        assert_eq!(runtime.consume_type.as_deref(), Some("CONSUME_PASSIVELY"));
        assert_eq!(runtime.consume_orderly, Some(false));
        assert_eq!(runtime.thread_pool_core_size, Some(20));
        assert_eq!(runtime.client_version.as_deref(), Some("V5_3_0"));
        assert_eq!(runtime.started_timestamp, Some(1_700_000_000_000));
        assert_eq!(
            runtime
                .subscriptions
                .iter()
                .map(|subscription| (subscription.topic.as_str(), subscription.expression.as_deref()))
                .collect::<Vec<_>>(),
            [("%RETRY%workers", Some("*")), ("orders", Some("TagA || TagB"))]
        );
        // A rate that is not a number is reported as zero, so the result stays valid JSON.
        assert_eq!(runtime.topic_status[0].pull_tps, 0.0);
        assert_eq!(runtime.topic_status[0].consume_failed_msgs, 3);

        assert_eq!(runtime.queue_count, 3);
        assert!(!runtime.truncated);
        let rows = runtime
            .queues
            .iter()
            .map(|row| (row.queue_id, row.mode, row.commit_offset, row.wait_ack_count))
            .collect::<Vec<_>>();
        assert_eq!(
            rows,
            [
                (0, ConsumerClientQueueMode::Pull, Some(30), None),
                (0, ConsumerClientQueueMode::Pop, None, Some(4)),
                (1, ConsumerClientQueueMode::Pull, Some(40), None),
            ]
        );
        assert!(runtime.queues[0].dropped);
        // A time the client never recorded is absent instead of the year 1970.
        assert_eq!(runtime.queues[0].last_pull_timestamp, None);
        assert_eq!(runtime.queues[1].last_pull_timestamp, Some(1_700_000_000_900));

        // Neither the property table nor the thread dump nor an address is carried over.
        let published = serde_json::to_string(&runtime).unwrap();
        for private in ["10.0.0.9", "accessKey", "private", "thread dump", MASTER] {
            assert!(!published.contains(private), "the result holds {private}");
        }
    }

    #[tokio::test]
    async fn a_client_no_instance_knows_and_a_broker_outside_the_cluster_are_errors() {
        let unknown = source(&[], reported());
        let error = query_consumer_client_runtime_from(&unknown, &request())
            .await
            .unwrap_err();
        assert_eq!(error.failure(), AdminFailure::OperationFailed);
        assert_eq!(unknown.calls.lock().unwrap().len(), 2);

        let elsewhere = source(&[MASTER], reported());
        let other_broker =
            QueryConsumerClientRuntimeRequest::try_new("cluster-a", "workers", "broker-z", "client-1").unwrap();
        let error = query_consumer_client_runtime_from(&elsewhere, &other_broker)
            .await
            .unwrap_err();
        assert_eq!(error.failure(), AdminFailure::NotFound);
        assert!(elsewhere.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_name_no_broker_would_accept_fails_the_read_and_an_overlong_expression_is_left_out() {
        let mut forged = reported();
        forged.subscription_set.insert(subscription("bad\u{0}topic", "*"));
        let source_forged = source(&[MASTER], forged);
        assert!(query_consumer_client_runtime_from(&source_forged, &request())
            .await
            .is_err());

        let mut long = ConsumerRunningInfo::new();
        long.subscription_set.insert(subscription(
            "orders",
            &"x".repeat(MAX_CONSUMER_CLIENT_EXPRESSION_BYTES + 1),
        ));
        let runtime = query_consumer_client_runtime_from(&source(&[MASTER], long), &request())
            .await
            .unwrap();
        assert_eq!(runtime.subscriptions[0].expression, None);
        // A client that reports nothing about itself has no property of its own.
        assert_eq!(runtime.consume_type, None);
        assert_eq!(runtime.started_timestamp, None);
    }
}
