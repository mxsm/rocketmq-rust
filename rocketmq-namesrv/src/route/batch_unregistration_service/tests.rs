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

use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_observability::TelemetryHandle;
use rocketmq_protocol::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
use rocketmq_runtime::RuntimeContext;
use rocketmq_transport::test_support::session_id_for_test;

use crate::bootstrap::Builder;
use crate::bootstrap::NameServerBootstrap;
use crate::route::types::BrokerSession;

use super::*;

#[cfg(feature = "otel-metrics")]
mod metrics_collection;

fn broker_request(index: u16) -> BrokerUnregistrationRequest {
    BrokerUnregistrationRequest::explicit(UnRegisterBrokerRequestHeader {
        cluster_name: CheetahString::from_static_str("cluster"),
        broker_addr: CheetahString::from_string(format!("127.0.0.1:{index}")),
        broker_name: CheetahString::from_string(format!("broker-{index}")),
        broker_id: u64::from(index),
    })
}

fn queued_requests(count: u16) -> (UnregistrationQueue, UnregistrationReceiver) {
    let (queue, rx) = UnregistrationQueue::new(8, NameServerMetrics::noop());
    for index in 0..count {
        assert!(matches!(queue.submit(broker_request(index)), Submission::Queued));
    }
    (queue, rx)
}

#[tokio::test]
async fn batch_collection_obeys_size_limit() {
    let (queue, mut rx) = queued_requests(5);
    let first = rx.recv_batch(2).await.unwrap();

    let batch = collect_batch(first, &mut rx, &CancellationToken::new(), 2, Duration::from_secs(1)).await;

    assert_eq!(batch.len(), 2);
    assert_eq!(queue.len(), 3);
}

#[tokio::test]
async fn batch_collection_returns_at_deadline_with_the_channel_open() {
    let (queue, mut rx) = queued_requests(2);
    let first = rx.recv_batch(8).await.unwrap();
    let cancellation = CancellationToken::new();
    let batch = collect_batch(first, &mut rx, &cancellation, 8, Duration::ZERO).await;

    assert_eq!(
        batch.requests().iter().map(|r| r.header.broker_id).collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert!(!queue.is_closed());
}

#[tokio::test]
async fn batch_collection_stops_when_the_sender_closes() {
    let (queue, mut rx) = queued_requests(2);
    let first = rx.recv_batch(8).await.unwrap();
    let cancellation = CancellationToken::new();
    let mut collecting = std::pin::pin!(collect_batch(first, &mut rx, &cancellation, 8, Duration::from_secs(10)));
    assert!(futures::poll!(&mut collecting).is_pending());
    drop(queue);
    let std::task::Poll::Ready(batch) = futures::poll!(&mut collecting) else {
        panic!("closed channel must not wait for the batch deadline");
    };
    assert_eq!(
        batch.requests().iter().map(|r| r.header.broker_id).collect::<Vec<_>>(),
        vec![0, 1]
    );
}

#[tokio::test]
async fn cancelled_batch_closes_the_receiver_and_drains_buffered_requests() {
    let (queue, mut rx) = queued_requests(4);
    let first = rx.recv_batch(8).await.unwrap();
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let batch = collect_batch(first, &mut rx, &cancellation, 8, Duration::ZERO).await;

    assert_eq!(
        batch.requests().iter().map(|r| r.header.broker_id).collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
    assert!(rx.is_closed());
    assert!(queue.is_closed());
    assert!(rx.recv_batch(8).await.is_none());
}

#[tokio::test]
async fn cancellation_during_collection_wakes_and_closes_the_receiver() {
    let (queue, mut rx) = queued_requests(1);
    let first = rx.recv_batch(8).await.unwrap();
    let cancellation = CancellationToken::new();
    {
        let mut collecting = std::pin::pin!(collect_batch(first, &mut rx, &cancellation, 8, Duration::from_secs(10)));
        assert!(futures::poll!(&mut collecting).is_pending());
        cancellation.cancel();
        let std::task::Poll::Ready(batch) = futures::poll!(&mut collecting) else {
            panic!("cancellation must wake collection without waiting for the deadline");
        };
        assert_eq!(
            batch.requests().iter().map(|r| r.header.broker_id).collect::<Vec<_>>(),
            vec![0]
        );
    }
    assert!(queue.is_closed());
    assert!(rx.recv_batch(8).await.is_none());
}

#[tokio::test]
async fn a_single_request_batch_leaves_buffered_requests_untouched() {
    let (queue, mut rx) = queued_requests(2);
    let first = rx.recv_batch(1).await.unwrap();
    let batch = collect_batch(first, &mut rx, &CancellationToken::new(), 1, Duration::from_secs(10)).await;

    assert_eq!(
        batch.requests().iter().map(|r| r.header.broker_id).collect::<Vec<_>>(),
        vec![0]
    );
    assert_eq!(queue.len(), 1);
    assert_eq!(rx.recv_batch(1).await.unwrap().requests()[0].header.broker_id, 1);
}

#[test]
fn pending_keys_separate_brokers_and_registration_generations() {
    let explicit = broker_request(1);
    let generation = BrokerGeneration {
        registration_epoch: 1,
        heartbeat_generation: 2,
    };
    let guarded = BrokerUnregistrationRequest::channel_guarded(explicit.header.clone(), "channel".into(), generation);
    let next_generation = BrokerUnregistrationRequest::channel_guarded(
        explicit.header.clone(),
        "channel".into(),
        BrokerGeneration {
            registration_epoch: 2,
            ..generation
        },
    );
    assert_eq!(explicit.pending_key(), broker_request(1).pending_key());
    assert_ne!(explicit.pending_key(), broker_request(2).pending_key());
    assert_ne!(explicit.pending_key(), guarded.pending_key());
    assert_ne!(guarded.pending_key(), next_generation.pending_key());
}

fn service_fixture(capacity: usize, batch_size: usize) -> (NameServerBootstrap, Arc<BatchUnregistrationService>) {
    let context = RuntimeContext::from_current("batch-unregistration-test");
    let bootstrap = Builder::new(context.service_context("namesrv"), TelemetryHandle::noop()).build();
    let service = Arc::new(BatchUnregistrationService::new(
        NameServerRuntimeHandle::new(&bootstrap.runtime_inner()),
        capacity,
        batch_size,
        Duration::from_millis(2),
        NameServerMetrics::noop(),
    ));
    (bootstrap, service)
}

fn register_broker(bootstrap: &NameServerBootstrap, index: u16) -> UnRegisterBrokerRequestHeader {
    let mut header = broker_request(index).header;
    header.broker_id = 0;
    bootstrap
        .runtime_inner()
        .route_info_manager()
        .register_broker_session(
            header.cluster_name.clone(),
            header.broker_addr.clone(),
            header.broker_name.clone(),
            header.broker_id,
            CheetahString::default(),
            None,
            None,
            Some(false),
            TopicConfigAndMappingSerializeWrapper::default(),
            vec![],
            BrokerSession::for_test(
                session_id_for_test(u64::from(index) + 1),
                format!("channel-{index}").into(),
                header.broker_addr.parse().unwrap(),
            ),
        )
        .unwrap();
    assert!(is_registered(bootstrap, &header));
    header
}

fn is_registered(bootstrap: &NameServerBootstrap, header: &UnRegisterBrokerRequestHeader) -> bool {
    bootstrap
        .runtime_inner()
        .route_info_manager()
        .query_broker_topic_config(header.cluster_name.clone(), header.broker_addr.clone())
        .is_some()
}

fn take_receiver(service: &BatchUnregistrationService) -> UnregistrationReceiver {
    match std::mem::replace(&mut *service.lifecycle.lock(), ServiceLifecycle::Failed) {
        ServiceLifecycle::Created(rx) => rx,
        _ => panic!("fixture worker must not have started"),
    }
}

#[tokio::test(start_paused = true)]
async fn immediate_shutdown_is_healthy_and_repeated_waits_retain_the_report() {
    let (_bootstrap, service) = service_fixture(8, 100);
    service.start().unwrap();
    let first = service.shutdown().await.unwrap();
    assert!(first.is_healthy(), "{}", first.to_json());
    assert_eq!(first.completed + first.cancelled, 1);
    let second = service.shutdown().await.unwrap();
    assert_eq!(first.to_json(), second.to_json());
    assert!(service.start().is_err());
}

#[tokio::test(start_paused = true)]
async fn shutdown_drains_multiple_batches_including_single_request_batches() {
    for batch_size in [1, 2, 100] {
        let (bootstrap, service) = service_fixture(8, batch_size);
        let headers = (0..7)
            .map(|index| register_broker(&bootstrap, index))
            .collect::<Vec<_>>();
        for header in &headers {
            assert!(service.submit(header.clone()));
        }
        service.start().unwrap();
        let report = service.shutdown().await.unwrap();
        assert!(report.is_healthy(), "{}", report.to_json());
        assert_eq!(service.queue.pending_len(), 0);
        assert_eq!(service.queue_length(), 0);
        assert!(headers.iter().all(|header| !is_registered(&bootstrap, header)));
    }
}

#[tokio::test(start_paused = true)]
async fn shutdown_before_start_drains_already_accepted_requests() {
    let (bootstrap, service) = service_fixture(8, 2);
    let header = register_broker(&bootstrap, 1);
    assert!(service.submit(header.clone()));
    assert!(service.shutdown().await.unwrap().is_healthy());
    assert!(!is_registered(&bootstrap, &header));
    assert_eq!(service.queue.pending_len(), 0);
}

#[tokio::test]
async fn empty_shutdown_before_start_closes_admission_and_preserves_closed_fallback() {
    let (bootstrap, service) = service_fixture(8, 2);
    assert!(service.shutdown().await.is_none());
    assert!(service.start().is_err());
    let header = register_broker(&bootstrap, 1);
    assert!(service.submit(header.clone()));
    assert!(!is_registered(&bootstrap, &header));
    assert_eq!(service.queue.pending_len(), 0);
    assert_eq!(service.queue_length(), 0);
}

#[tokio::test]
async fn full_queue_fallback_and_coalescing_preserve_the_accepted_request() {
    let (bootstrap, service) = service_fixture(1, 2);
    let first = register_broker(&bootstrap, 1);
    let second = register_broker(&bootstrap, 2);
    assert!(service.submit(first.clone()));
    assert!(service.submit(first.clone()));
    assert!(service.submit(second.clone()));
    assert_eq!(service.queue_length(), 1);
    assert_eq!(service.queue.pending_len(), 1);
    assert!(is_registered(&bootstrap, &first));
    assert!(!is_registered(&bootstrap, &second));
    assert!(service.shutdown().await.unwrap().is_healthy());
    assert!(!is_registered(&bootstrap, &first));
}

#[tokio::test]
async fn dropping_a_partial_batch_clears_pending_before_a_closed_queue_retry() {
    let (bootstrap, service) = service_fixture(8, 100);
    let header = register_broker(&bootstrap, 1);
    assert!(service.submit(header.clone()));
    let rx = take_receiver(&service);
    let mut worker = Box::pin(run_batch_unregistration_service(
        service.name_server_runtime_inner.clone(),
        rx,
        CancellationToken::new(),
        100,
        Duration::from_secs(10),
        NameServerMetrics::noop(),
    ));
    assert!(futures::poll!(&mut worker).is_pending());
    assert_eq!(service.queue_length(), 0);
    assert_eq!(service.queue.pending_len(), 1);
    drop(worker);
    assert_eq!(service.queue.pending_len(), 0);
    assert!(is_registered(&bootstrap, &header));
    assert!(service.submit(header.clone()));
    assert!(!is_registered(&bootstrap, &header));
    assert_eq!(service.queue.pending_len(), 0);
}

#[tokio::test]
async fn cancelling_collection_releases_only_its_batch_and_allows_retry() {
    let (queue, mut rx) = queued_requests(3);
    let batch = rx.recv_batch(8).await.unwrap();
    let cancellation = CancellationToken::new();
    let mut collecting = Box::pin(collect_batch(batch, &mut rx, &cancellation, 8, Duration::from_secs(10)));
    assert!(futures::poll!(&mut collecting).is_pending());
    assert_eq!(queue.len(), 0);
    assert_eq!(queue.pending_len(), 3);
    // This request stays buffered when collection is cancelled.
    assert!(matches!(queue.submit(broker_request(3)), Submission::Queued));
    drop(collecting);
    assert_eq!(queue.pending_len(), 1);
    for index in 0..3 {
        assert!(matches!(queue.submit(broker_request(index)), Submission::Queued));
    }
    drop(rx);
    assert_eq!(queue.pending_len(), 0);
}

#[tokio::test]
async fn dropping_unpolled_collection_releases_its_first_request() {
    let (queue, mut rx) = queued_requests(2);
    let batch = rx.recv_batch(8).await.unwrap();
    let cancellation = CancellationToken::new();
    drop(collect_batch(batch, &mut rx, &cancellation, 8, Duration::from_secs(10)));
    assert_eq!(queue.pending_len(), 1);
    assert!(matches!(queue.submit(broker_request(0)), Submission::Queued));
}

#[tokio::test]
async fn dropping_an_unpolled_worker_releases_buffered_requests() {
    let (_bootstrap, service) = service_fixture(8, 100);
    for index in 0..4 {
        assert!(service.submit(broker_request(index).header));
    }
    let worker = run_batch_unregistration_service(
        service.name_server_runtime_inner.clone(),
        take_receiver(&service),
        CancellationToken::new(),
        100,
        Duration::from_secs(10),
        NameServerMetrics::noop(),
    );
    drop(worker);
    assert_eq!(service.queue_length(), 0);
    assert_eq!(service.queue.pending_len(), 0);
    assert_eq!(service.queue.oldest_age(), Duration::ZERO);
}

#[tokio::test]
async fn cancellation_between_batches_closes_and_drains_the_worker() {
    let (bootstrap, service) = service_fixture(8, 2);
    let headers = (0..5)
        .map(|index| register_broker(&bootstrap, index))
        .collect::<Vec<_>>();
    for header in &headers {
        assert!(service.submit(header.clone()));
    }
    let cancellation = CancellationToken::new();
    let mut worker = Box::pin(run_batch_unregistration_service(
        service.name_server_runtime_inner.clone(),
        take_receiver(&service),
        cancellation.clone(),
        2,
        Duration::from_secs(10),
        NameServerMetrics::noop(),
    ));
    assert!(futures::poll!(&mut worker).is_pending());
    assert!(!is_registered(&bootstrap, &headers[0]));
    cancellation.cancel();
    for _ in 0..5 {
        if futures::poll!(&mut worker).is_ready() {
            assert!(headers.iter().all(|header| !is_registered(&bootstrap, header)));
            assert_eq!(service.queue.pending_len(), 0);
            return;
        }
    }
    panic!("cancelled worker must finish without waiting for new requests or a batch deadline");
}

#[tokio::test(start_paused = true)]
async fn cancelled_shutdown_wait_can_be_resumed() {
    let (bootstrap, service) = service_fixture(8, 100);
    let header = register_broker(&bootstrap, 1);
    assert!(service.submit(header.clone()));
    service.start().unwrap();
    let mut first_wait = Box::pin(service.shutdown());
    assert!(futures::poll!(&mut first_wait).is_pending());
    drop(first_wait);
    assert!(service.shutdown().await.unwrap().is_healthy());
    assert!(!is_registered(&bootstrap, &header));
    assert_eq!(service.queue.pending_len(), 0);
}

#[tokio::test(start_paused = true)]
async fn concurrent_shutdown_callers_receive_the_same_report() {
    let (_bootstrap, service) = service_fixture(8, 100);
    service.start().unwrap();
    let (first, second) = tokio::join!(service.shutdown(), service.shutdown());
    let first = first.unwrap();
    assert!(first.is_healthy());
    assert_eq!(first.to_json(), second.unwrap().to_json());
}

#[tokio::test]
async fn concurrent_starts_create_one_worker() {
    let (_bootstrap, service) = service_fixture(8, 100);
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let service = &service;
            scope.spawn(move || service.start().unwrap());
        }
    });
    {
        let lifecycle = service.lifecycle.lock();
        let ServiceLifecycle::Running(group) = &*lifecycle else {
            panic!("service should have started");
        };
        assert_eq!(group.task_count(), 1);
    }
    assert!(service.shutdown().await.unwrap().is_healthy());
}

#[tokio::test]
async fn failed_spawn_releases_queued_keys_and_keeps_synchronous_fallback() {
    let (bootstrap, service) = service_fixture(8, 100);
    let header = register_broker(&bootstrap, 1);
    assert!(service.submit(header.clone()));
    bootstrap
        .runtime_inner()
        .task_group()
        .shutdown(Duration::from_secs(1))
        .await;
    assert!(service.start().is_err());
    assert_eq!(service.queue.pending_len(), 0);
    assert!(service.submit(header.clone()));
    assert!(!is_registered(&bootstrap, &header));
}

#[tokio::test]
async fn releasing_runtime_drops_the_batch_and_remaining_queue() {
    let (bootstrap, service) = service_fixture(8, 1);
    for index in 0..3 {
        assert!(service.submit(broker_request(index).header));
    }
    let rx = take_receiver(&service);
    drop(bootstrap);
    assert!(service.name_server_runtime_inner.upgrade().is_none());
    run_batch_unregistration_service(
        service.name_server_runtime_inner.clone(),
        rx,
        CancellationToken::new(),
        1,
        Duration::from_secs(10),
        NameServerMetrics::noop(),
    )
    .await;
    assert_eq!(service.queue.pending_len(), 0);
    assert_eq!(service.queue_length(), 0);
    assert!(!service.submit(broker_request(0).header));
}

#[tokio::test(start_paused = true)]
async fn queue_metrics_track_queued_and_inflight_work_and_reset_after_drop() {
    #[cfg(feature = "otel-metrics")]
    let telemetry = {
        let mut config = rocketmq_observability::ObservabilityConfig {
            enabled: true,
            ..Default::default()
        };
        config.metrics.enabled = true;
        config.metrics.exporter = rocketmq_observability::MetricsExporter::Log;
        config.metrics.export_interval_millis = 3_600_000;
        rocketmq_observability::init_observability(&config).unwrap()
    };
    #[cfg(feature = "otel-metrics")]
    let metrics = NameServerMetrics::from_handle(&telemetry.handle());
    #[cfg(not(feature = "otel-metrics"))]
    let metrics = NameServerMetrics::noop();
    let age_tracking = metrics.is_enabled();
    #[cfg(feature = "otel-metrics")]
    assert!(age_tracking);
    let (queue, mut rx) = UnregistrationQueue::new(2, metrics);
    let delayed = broker_request(0);
    tokio::time::advance(Duration::from_millis(20)).await;
    assert!(matches!(queue.submit(delayed), Submission::Queued));
    assert_eq!(queue.oldest_age(), Duration::ZERO);
    tokio::time::advance(Duration::from_millis(10)).await;
    assert!(matches!(queue.submit(broker_request(0)), Submission::Coalesced));
    assert!(matches!(queue.submit(broker_request(1)), Submission::Queued));
    assert!(matches!(
        queue.submit(broker_request(2)),
        Submission::Fallback(queue::QueueUnavailable::Full, _)
    ));
    let first = rx.recv_batch(1).await.unwrap();
    assert_eq!(queue.len(), 1);
    assert_eq!(queue.pending_len(), 2);
    assert_eq!(
        queue.oldest_age(),
        if age_tracking {
            Duration::from_millis(10)
        } else {
            Duration::ZERO
        }
    );
    drop(first);
    assert_eq!(queue.oldest_age(), Duration::ZERO);
    tokio::time::advance(Duration::from_millis(3)).await;
    assert_eq!(
        queue.oldest_age(),
        if age_tracking {
            Duration::from_millis(3)
        } else {
            Duration::ZERO
        }
    );
    let inflight = rx.recv_batch(1).await.unwrap();
    assert!(matches!(queue.submit(broker_request(2)), Submission::Queued));
    drop(rx);
    assert_eq!(queue.len(), 0);
    assert_eq!(queue.pending_len(), 0);
    assert_eq!(queue.oldest_age(), Duration::ZERO);
    drop(inflight);
    assert_eq!(queue.oldest_age(), Duration::ZERO);
}

#[test]
fn concurrent_full_submissions_never_coalesce_into_a_rejected_request() {
    let (queue, _rx) = UnregistrationQueue::new(1, NameServerMetrics::noop());
    assert!(matches!(queue.submit(broker_request(0)), Submission::Queued));
    let gate = std::sync::Barrier::new(4);
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let queue = &queue;
            let gate = &gate;
            scope.spawn(move || {
                gate.wait();
                for _ in 0..64 {
                    assert!(matches!(
                        queue.submit(broker_request(1)),
                        Submission::Fallback(queue::QueueUnavailable::Full, _)
                    ));
                }
            });
        }
    });
    assert_eq!(queue.pending_len(), 1);
    assert_eq!(queue.len(), 1);
}

#[tokio::test]
async fn concurrent_duplicates_share_one_registration_until_the_batch_is_released() {
    let (queue, mut rx) = UnregistrationQueue::new(1, NameServerMetrics::noop());
    assert!(matches!(queue.submit(broker_request(0)), Submission::Queued));
    let gate = std::sync::Barrier::new(4);
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let queue = &queue;
            let gate = &gate;
            scope.spawn(move || {
                gate.wait();
                for _ in 0..128 {
                    assert!(matches!(queue.submit(broker_request(0)), Submission::Coalesced));
                }
            });
        }
    });
    assert_eq!(queue.pending_len(), 1);
    let batch = rx.recv_batch(1).await.unwrap();
    assert!(matches!(queue.submit(broker_request(0)), Submission::Coalesced));
    drop(batch);
    assert_eq!(queue.pending_len(), 0);
    assert!(matches!(queue.submit(broker_request(0)), Submission::Queued));
}

#[test]
fn receiver_drop_fences_concurrent_admission_and_clears_all_keys() {
    for _ in 0..16 {
        let (queue, rx) = UnregistrationQueue::new(1024, NameServerMetrics::noop());
        let gate = std::sync::Barrier::new(5);
        std::thread::scope(|scope| {
            for thread in 0..4 {
                let queue = &queue;
                let gate = &gate;
                scope.spawn(move || {
                    gate.wait();
                    for index in 0..128 {
                        let result = queue.submit(broker_request(thread * 128 + index));
                        assert!(matches!(
                            result,
                            Submission::Queued | Submission::Fallback(queue::QueueUnavailable::Closed, _)
                        ));
                    }
                });
            }
            gate.wait();
            drop(rx);
        });
        assert_eq!(queue.len(), 0);
        assert_eq!(queue.pending_len(), 0);
        assert!(matches!(
            queue.submit(broker_request(0)),
            Submission::Fallback(queue::QueueUnavailable::Closed, _)
        ));
    }
}

#[tokio::test]
async fn unwinding_batch_execution_releases_the_pending_registration() {
    let (queue, mut rx) = UnregistrationQueue::new(1, NameServerMetrics::noop());
    assert!(matches!(queue.submit(broker_request(0)), Submission::Queued));
    let (_, registration) = rx.recv_batch(1).await.unwrap().into_parts();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _registration = registration;
        panic!("injected batch execution failure");
    }));
    assert!(result.is_err());
    assert_eq!(queue.pending_len(), 0);
    assert!(matches!(queue.submit(broker_request(0)), Submission::Queued));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_admission_and_shutdown_remove_every_broker() {
    for capacity in [1, 32] {
        for batch_size in [1, 2, 100] {
            let (bootstrap, service) = service_fixture(capacity, batch_size);
            let headers = (0..17)
                .map(|index| register_broker(&bootstrap, index))
                .collect::<Vec<_>>();
            assert!(service.submit(headers[0].clone()));
            service.start().unwrap();
            let ready = Arc::new(std::sync::Barrier::new(5));
            let mut producers = Vec::new();
            for group in headers[1..].chunks(4) {
                let service = Arc::clone(&service);
                let ready = Arc::clone(&ready);
                let requests = group.to_vec();
                producers.push(std::thread::spawn(move || {
                    assert!(service.submit(requests[0].clone()));
                    ready.wait();
                    for request in &requests[1..] {
                        assert!(service.submit(request.clone()));
                    }
                }));
            }
            ready.wait();
            assert!(service.shutdown().await.unwrap().is_healthy());
            // Synchronous fallbacks are caller-owned, so join their producers
            // before checking the final routing state.
            for producer in producers {
                producer.join().unwrap();
            }
            assert!(headers.iter().all(|header| !is_registered(&bootstrap, header)));
            assert_eq!(service.queue.pending_len(), 0);
            assert_eq!(service.queue_length(), 0);
        }
    }
}
