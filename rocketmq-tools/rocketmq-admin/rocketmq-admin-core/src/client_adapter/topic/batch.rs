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

use std::collections::HashSet;

use cheetah_string::CheetahString;
use rocketmq_client_rust::RouteAdmin as _;
use rocketmq_client_rust::TopicAdmin as _;

use crate::client_adapter::lifecycle::AdminSession;
use crate::core::stable_error_message;
use crate::core::topic::CanonicalTopicBatchUpsertRequest;
use crate::core::topic::DeleteTopicAdminRequest;
use crate::core::topic::TopicAdmin;
use crate::core::topic::TopicBatchDeleteAdmin;
use crate::core::topic::TopicBatchDeleteOutcome;
use crate::core::topic::TopicBatchDeleteRequest;
use crate::core::topic::TopicBatchMutationAdmin;
use crate::core::topic::TopicBatchMutationOutcome;
use crate::core::topic::TopicBatchOrderConfigOutcome;
use crate::core::topic::TopicBatchTargetOutcome;
use crate::core::topic::TopicBatchUpsertRequest;
use crate::core::topic::TopicMutationOutcome;
use crate::core::topic::UpsertTopicRequest;
use crate::core::AdminFuture;
use crate::core::AdminResult;

use super::backend_error;
use super::build_order_conf;

impl TopicBatchMutationAdmin for AdminSession {
    fn upsert_topic_batch<'a>(
        &'a mut self,
        request: &'a TopicBatchUpsertRequest,
    ) -> AdminFuture<'a, TopicBatchMutationOutcome> {
        Box::pin(async move { run_topic_batch_workflow(self, request).await })
    }
}

impl TopicBatchDeleteAdmin for AdminSession {
    fn delete_topic_batch<'a>(
        &'a mut self,
        request: &'a TopicBatchDeleteRequest,
    ) -> AdminFuture<'a, TopicBatchDeleteOutcome> {
        Box::pin(async move { run_topic_batch_delete_workflow(self, request).await })
    }
}

trait TopicBatchDeleteExecutor {
    fn delete_topic_cluster<'a>(
        &'a mut self,
        request: &'a DeleteTopicAdminRequest,
    ) -> AdminFuture<'a, TopicMutationOutcome>;

    fn delete_order_config<'a>(&'a mut self, topic: &'a str) -> AdminFuture<'a, ()>;
}

impl TopicBatchDeleteExecutor for AdminSession {
    fn delete_topic_cluster<'a>(
        &'a mut self,
        request: &'a DeleteTopicAdminRequest,
    ) -> AdminFuture<'a, TopicMutationOutcome> {
        TopicAdmin::delete_topic(self, request)
    }

    fn delete_order_config<'a>(&'a mut self, topic: &'a str) -> AdminFuture<'a, ()> {
        Box::pin(async move {
            self.ensure_open()?;
            self.inner
                .delete_kv_config(
                    CheetahString::from_static_str("ORDER_TOPIC_CONFIG"),
                    CheetahString::from(topic),
                )
                .await
                .map_err(|error| backend_error("delete_order_topic_config", error))
        })
    }
}

async fn run_topic_batch_delete_workflow<E>(
    executor: &mut E,
    request: &TopicBatchDeleteRequest,
) -> AdminResult<TopicBatchDeleteOutcome>
where
    E: TopicBatchDeleteExecutor,
{
    let request = request.canonical_for_execution()?;
    let mut targets = Vec::with_capacity(request.cluster_names().len());
    for cluster_name in request.cluster_names() {
        match executor
            .delete_topic_cluster(&DeleteTopicAdminRequest {
                topic: request.topic().to_string(),
                cluster_name: Some(cluster_name.clone()),
                broker_name: None,
            })
            .await
        {
            Ok(outcome) => targets.push(TopicBatchTargetOutcome {
                broker_name: cluster_name.clone(),
                success: true,
                message: outcome.message,
            }),
            Err(error) => targets.push(TopicBatchTargetOutcome {
                broker_name: cluster_name.clone(),
                success: false,
                message: stable_error_message(&error),
            }),
        }
    }
    let order_config = if targets.iter().all(|target| target.success) {
        Some(match executor.delete_order_config(request.topic()).await {
            Ok(()) => TopicBatchOrderConfigOutcome {
                success: true,
                message: "Order topic configuration deleted".to_string(),
            },
            Err(error) => TopicBatchOrderConfigOutcome {
                success: false,
                message: stable_error_message(&error),
            },
        })
    } else {
        None
    };
    Ok(TopicBatchDeleteOutcome { targets, order_config })
}

trait TopicBatchExecutor {
    fn upsert_topic_local<'a>(&'a mut self, request: &'a UpsertTopicRequest) -> AdminFuture<'a, TopicMutationOutcome>;

    fn reconcile_order_config<'a>(
        &'a mut self,
        request: &'a CanonicalTopicBatchUpsertRequest,
        successful_brokers: &'a [String],
    ) -> AdminFuture<'a, ()>;
}

impl TopicBatchExecutor for AdminSession {
    fn upsert_topic_local<'a>(&'a mut self, request: &'a UpsertTopicRequest) -> AdminFuture<'a, TopicMutationOutcome> {
        Box::pin(async move { self.upsert_topic_config(request, false).await })
    }

    fn reconcile_order_config<'a>(
        &'a mut self,
        request: &'a CanonicalTopicBatchUpsertRequest,
        successful_brokers: &'a [String],
    ) -> AdminFuture<'a, ()> {
        Box::pin(async move {
            self.reconcile_order_topic_config_internal(request, successful_brokers)
                .await
        })
    }
}

async fn run_topic_batch_workflow<E>(
    executor: &mut E,
    request: &TopicBatchUpsertRequest,
) -> AdminResult<TopicBatchMutationOutcome>
where
    E: TopicBatchExecutor,
{
    let request = request.canonical_for_execution()?;
    let mut targets = Vec::with_capacity(request.broker_names.len());
    for broker_name in &request.broker_names {
        let local_request = UpsertTopicRequest {
            cluster_names: Vec::new(),
            broker_names: vec![broker_name.clone()],
            topic: request.topic.clone(),
            write_queue_nums: request.write_queue_nums,
            read_queue_nums: request.read_queue_nums,
            perm: request.perm,
            order: request.order,
            message_type: request.message_type.clone(),
        };
        match executor.upsert_topic_local(&local_request).await {
            Ok(outcome) => targets.push(TopicBatchTargetOutcome {
                broker_name: broker_name.clone(),
                success: true,
                message: outcome.message,
            }),
            Err(error) => targets.push(TopicBatchTargetOutcome {
                broker_name: broker_name.clone(),
                success: false,
                message: stable_error_message(&error),
            }),
        }
    }
    let successful_brokers = targets
        .iter()
        .filter(|target| target.success)
        .map(|target| target.broker_name.clone())
        .collect::<Vec<_>>();
    let order_config = if successful_brokers.is_empty() {
        None
    } else {
        Some(
            match executor.reconcile_order_config(&request, &successful_brokers).await {
                Ok(()) => TopicBatchOrderConfigOutcome {
                    success: true,
                    message: "Order topic configuration reconciled".to_string(),
                },
                Err(error) => TopicBatchOrderConfigOutcome {
                    success: false,
                    message: stable_error_message(&error),
                },
            },
        )
    };
    Ok(TopicBatchMutationOutcome { targets, order_config })
}

impl AdminSession {
    async fn reconcile_order_topic_config_internal(
        &mut self,
        request: &CanonicalTopicBatchUpsertRequest,
        successful_brokers: &[String],
    ) -> AdminResult<()> {
        self.ensure_open()?;
        if request.order {
            let broker_names = successful_brokers.iter().cloned().collect::<HashSet<_>>();
            let order_conf = build_order_conf(&broker_names, request.write_queue_nums);
            self.inner
                .create_or_update_order_conf(
                    CheetahString::from(request.topic.as_str()),
                    CheetahString::from(order_conf),
                    true,
                )
                .await
                .map_err(|error| backend_error("create_or_update_order_conf", error))
        } else {
            self.inner
                .delete_kv_config(
                    CheetahString::from_static_str("ORDER_TOPIC_CONFIG"),
                    CheetahString::from(request.topic.as_str()),
                )
                .await
                .map_err(|error| backend_error("delete_order_topic_config", error))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::run_topic_batch_delete_workflow;
    use super::run_topic_batch_workflow;
    use super::TopicBatchDeleteExecutor;
    use super::TopicBatchExecutor;
    use crate::core::topic::CanonicalTopicBatchUpsertRequest;
    use crate::core::topic::DeleteTopicAdminRequest;
    use crate::core::topic::TopicBatchDeleteRequest;
    use crate::core::topic::TopicBatchUpsertRequest;
    use crate::core::topic::TopicMutationOutcome;
    use crate::core::topic::UpsertTopicRequest;
    use crate::core::AdminError;
    use crate::core::AdminFuture;

    #[tokio::test]
    async fn batch_workflow_reconciles_order_once_with_the_complete_successful_set() {
        let request = batch_request(true);
        let mut executor = RecordingBatchExecutor::default();

        let outcome = run_topic_batch_workflow(&mut executor, &request)
            .await
            .expect("batch workflow");

        assert_eq!(executor.local_targets, ["broker-a", "broker-b"]);
        assert_eq!(
            executor.reconciliations,
            vec![(true, vec!["broker-a".to_string(), "broker-b".to_string()])]
        );
        assert!(outcome.targets.iter().all(|target| target.success));
        assert!(outcome.order_config.expect("order result").success);
    }

    #[tokio::test]
    async fn batch_workflow_deletes_order_config_for_unordered_topic() {
        let request = batch_request(false);
        let mut executor = RecordingBatchExecutor::default();

        let outcome = run_topic_batch_workflow(&mut executor, &request)
            .await
            .expect("batch workflow");

        assert_eq!(
            executor.reconciliations,
            vec![(false, vec!["broker-a".to_string(), "broker-b".to_string()])]
        );
        assert!(outcome.order_config.expect("delete result").success);
    }

    #[tokio::test]
    async fn batch_workflow_reconciles_only_successful_brokers_after_a_partial_failure() {
        let request = batch_request(true);
        let mut executor = RecordingBatchExecutor {
            failing_target: Some("broker-a".into()),
            ..Default::default()
        };

        let outcome = run_topic_batch_workflow(&mut executor, &request)
            .await
            .expect("structured partial batch result");

        assert_eq!(executor.local_targets, ["broker-a", "broker-b"]);
        assert_eq!(executor.reconciliations, vec![(true, vec!["broker-b".to_string()])]);
        assert!(!outcome.targets[0].success);
        assert!(outcome.targets[1].success);
    }

    #[tokio::test]
    async fn batch_workflow_skips_reconciliation_when_every_local_update_fails() {
        let request = batch_request(true);
        let mut executor = RecordingBatchExecutor {
            fail_all: true,
            ..Default::default()
        };

        let outcome = run_topic_batch_workflow(&mut executor, &request)
            .await
            .expect("structured all-failure batch result");

        assert_eq!(executor.local_targets, ["broker-a", "broker-b"]);
        assert!(executor.reconciliations.is_empty());
        assert!(outcome.order_config.is_none());
    }

    #[tokio::test]
    async fn batch_delete_cleans_order_config_once_only_after_all_clusters_succeed() {
        let request = TopicBatchDeleteRequest::try_new("orders", vec!["cluster-b".into(), "cluster-a".into()])
            .expect("validated delete request");
        let mut executor = RecordingDeleteExecutor::default();

        let outcome = run_topic_batch_delete_workflow(&mut executor, &request)
            .await
            .expect("structured delete outcome");

        assert_eq!(executor.clusters, ["cluster-a", "cluster-b"]);
        assert_eq!(executor.order_cleanup_calls, 1);
        assert!(outcome.order_config.expect("cleanup result").success);
    }

    #[tokio::test]
    async fn batch_delete_skips_order_cleanup_for_partial_or_total_cluster_failure() {
        let request = TopicBatchDeleteRequest::try_new("orders", vec!["cluster-a".into(), "cluster-b".into()])
            .expect("validated delete request");
        for failing_cluster in [Some("cluster-a"), None] {
            let mut executor = RecordingDeleteExecutor {
                failing_cluster: failing_cluster.map(str::to_string),
                fail_all: failing_cluster.is_none(),
                ..Default::default()
            };

            let outcome = run_topic_batch_delete_workflow(&mut executor, &request)
                .await
                .expect("structured delete outcome");

            assert_eq!(executor.order_cleanup_calls, 0);
            assert!(outcome.order_config.is_none());
            assert!(outcome.targets.iter().any(|target| !target.success));
        }
    }

    #[tokio::test]
    async fn batch_delete_reports_order_cleanup_failure_after_all_clusters_succeed() {
        let request =
            TopicBatchDeleteRequest::try_new("orders", vec!["cluster-a".into()]).expect("validated delete request");
        let mut executor = RecordingDeleteExecutor {
            fail_order_cleanup: true,
            ..Default::default()
        };

        let outcome = run_topic_batch_delete_workflow(&mut executor, &request)
            .await
            .expect("structured delete outcome");

        assert_eq!(executor.order_cleanup_calls, 1);
        assert!(!outcome.order_config.expect("cleanup result").success);
    }

    #[tokio::test]
    async fn batch_workflow_revalidates_unchecked_input_before_any_side_effect() {
        let requests = [
            TopicBatchUpsertRequest::unchecked_for_execution_test(
                "orders topic".into(),
                vec!["broker-a".into()],
                8,
                8,
                6,
                true,
                None,
            ),
            TopicBatchUpsertRequest::unchecked_for_execution_test(
                "orders".into(),
                vec!["broker:a".into()],
                8,
                8,
                6,
                true,
                None,
            ),
            TopicBatchUpsertRequest::unchecked_for_execution_test(
                "orders".into(),
                vec!["broker-a".into()],
                0,
                8,
                6,
                true,
                None,
            ),
        ];
        for request in requests {
            let mut executor = RecordingBatchExecutor::default();

            assert!(run_topic_batch_workflow(&mut executor, &request).await.is_err());
            assert!(executor.local_targets.is_empty());
            assert!(executor.reconciliations.is_empty());
        }
    }

    fn batch_request(order: bool) -> TopicBatchUpsertRequest {
        TopicBatchUpsertRequest::try_new(
            "orders",
            vec!["broker-b".into(), "broker-a".into()],
            8,
            8,
            6,
            order,
            Some("NORMAL".into()),
        )
        .expect("valid batch request")
    }

    #[derive(Default)]
    struct RecordingBatchExecutor {
        local_targets: Vec<String>,
        reconciliations: Vec<(bool, Vec<String>)>,
        failing_target: Option<String>,
        fail_all: bool,
    }

    impl TopicBatchExecutor for RecordingBatchExecutor {
        fn upsert_topic_local<'a>(
            &'a mut self,
            request: &'a UpsertTopicRequest,
        ) -> AdminFuture<'a, TopicMutationOutcome> {
            self.local_targets.push(request.broker_names[0].clone());
            let should_fail =
                self.fail_all || self.failing_target.as_deref() == request.broker_names.first().map(String::as_str);
            Box::pin(async move {
                if should_fail {
                    Err(AdminError::backend("local_topic_update", "unavailable"))
                } else {
                    Ok(TopicMutationOutcome {
                        message: "saved".to_string(),
                        target_count: 1,
                    })
                }
            })
        }

        fn reconcile_order_config<'a>(
            &'a mut self,
            request: &'a CanonicalTopicBatchUpsertRequest,
            successful_brokers: &'a [String],
        ) -> AdminFuture<'a, ()> {
            self.reconciliations.push((request.order, successful_brokers.to_vec()));
            Box::pin(async { Ok(()) })
        }
    }

    #[derive(Default)]
    struct RecordingDeleteExecutor {
        clusters: Vec<String>,
        order_cleanup_calls: usize,
        failing_cluster: Option<String>,
        fail_all: bool,
        fail_order_cleanup: bool,
    }

    impl TopicBatchDeleteExecutor for RecordingDeleteExecutor {
        fn delete_topic_cluster<'a>(
            &'a mut self,
            request: &'a DeleteTopicAdminRequest,
        ) -> AdminFuture<'a, TopicMutationOutcome> {
            let cluster = request.cluster_name.clone().expect("test cluster request");
            self.clusters.push(cluster.clone());
            let should_fail = self.fail_all || self.failing_cluster.as_deref() == Some(cluster.as_str());
            Box::pin(async move {
                if should_fail {
                    Err(AdminError::backend("delete_topic", "unavailable"))
                } else {
                    Ok(TopicMutationOutcome {
                        message: "deleted".to_string(),
                        target_count: 1,
                    })
                }
            })
        }

        fn delete_order_config<'a>(&'a mut self, _: &'a str) -> AdminFuture<'a, ()> {
            self.order_cleanup_calls += 1;
            let fail = self.fail_order_cleanup;
            Box::pin(async move {
                if fail {
                    Err(AdminError::backend("delete_order_topic_config", "unavailable"))
                } else {
                    Ok(())
                }
            })
        }
    }
}
