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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Weak;

use rocketmq_model::common::config::TopicConfig;
use rocketmq_observability::metrics::namesrv::UnregistrationAgeSource;

use super::*;

struct CollectionProbe {
    service: Weak<BatchUnregistrationService>,
    armed: AtomicBool,
    samples: mpsc::Sender<Option<(usize, Duration)>>,
    resume: parking_lot::Mutex<Option<mpsc::Receiver<()>>>,
}

impl UnregistrationAgeSource for CollectionProbe {
    fn oldest_age(&self) -> Duration {
        if !self.armed.load(Ordering::Acquire) {
            return Duration::ZERO;
        }
        let sample = self
            .service
            .upgrade()
            .map(|service| (service.queue.pending_len(), service.queue.oldest_age()));
        let _ = self.samples.send(sample);
        let resume = self.resume.lock().take();
        if let Some(resume) = resume {
            // Keep the SDK collection in progress, without retaining the service.
            // Dropping the sender also releases collection if the test unwinds.
            let _ = resume.recv();
        }
        Duration::ZERO
    }
}

struct Fixture {
    // Unblock collection before dropping its provider if an assertion unwinds.
    resume: mpsc::Sender<()>,
    context: RuntimeContext,
    bootstrap: NameServerBootstrap,
    service: Arc<BatchUnregistrationService>,
    telemetry: rocketmq_observability::TelemetryRuntimeGuard,
    probe: Arc<CollectionProbe>,
    samples: mpsc::Receiver<Option<(usize, Duration)>>,
    headers: Vec<UnRegisterBrokerRequestHeader>,
}

fn fixture(batch_size: usize, batch_time: Duration) -> Fixture {
    let mut config = rocketmq_observability::ObservabilityConfig {
        enabled: true,
        ..Default::default()
    };
    config.metrics.enabled = true;
    config.metrics.exporter = rocketmq_observability::MetricsExporter::Log;
    config.metrics.export_interval_millis = 1;
    let telemetry = rocketmq_observability::init_observability(&config).unwrap();
    let metrics = NameServerMetrics::from_handle(&telemetry.handle());
    let context = RuntimeContext::from_current("unregistration-collection-test");
    let bootstrap = Builder::new(context.service_context("namesrv"), telemetry.handle()).build();
    let service = Arc::new(BatchUnregistrationService::new(
        NameServerRuntimeHandle::new(&bootstrap.runtime_inner()),
        64,
        batch_size,
        batch_time,
        metrics.clone(),
    ));
    let headers = (0..32)
        .map(|index| {
            let mut header = broker_request(index).header;
            header.broker_id = 0;
            let mut wrapper = TopicConfigAndMappingSerializeWrapper::default();
            let topic: CheetahString = format!("collection-topic-{index}").into();
            wrapper
                .topic_config_serialize_wrapper
                .topic_config_table
                .insert(topic.clone(), TopicConfig::with_perm(topic, 4, 4, 6));
            bootstrap
                .runtime_inner()
                .route_info_manager()
                .register_broker_session(
                    header.cluster_name.clone(),
                    header.broker_addr.clone(),
                    header.broker_name.clone(),
                    0,
                    CheetahString::default(),
                    None,
                    None,
                    Some(false),
                    wrapper,
                    vec![],
                    BrokerSession::for_test(
                        session_id_for_test(u64::from(index) + 1),
                        format!("collection-channel-{index}").into(),
                        header.broker_addr.parse().unwrap(),
                    ),
                )
                .unwrap();
            header
        })
        .collect();
    let (sample_tx, samples) = mpsc::channel();
    let (resume, resume_rx) = mpsc::channel();
    let probe = Arc::new(CollectionProbe {
        service: Arc::downgrade(&service),
        armed: AtomicBool::new(false),
        samples: sample_tx,
        resume: parking_lot::Mutex::new(Some(resume_rx)),
    });
    metrics.register_unregistration_age_source(&probe);
    Fixture {
        context,
        bootstrap,
        service,
        telemetry,
        probe,
        samples,
        resume,
        headers,
    }
}

fn sample(samples: &mpsc::Receiver<Option<(usize, Duration)>>) -> Option<(usize, Duration)> {
    samples.recv_timeout(Duration::from_secs(5)).unwrap()
}

async fn finish(fixture: Fixture) {
    let Fixture {
        context,
        bootstrap,
        service,
        telemetry,
        probe,
        samples,
        resume,
        ..
    } = fixture;
    assert_eq!(service.queue.pending_len(), 0);
    assert_eq!(service.queue_length(), 0);
    assert_eq!(service.queue.oldest_age(), Duration::ZERO);
    resume.send(()).unwrap();
    assert_eq!(sample(&samples), Some((0, Duration::ZERO)));
    let weak_service = Arc::downgrade(&service);
    drop(service);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while sample(&samples).is_some() {
        assert!(std::time::Instant::now() < deadline, "collector retained the service");
    }
    assert!(weak_service.upgrade().is_none());
    let weak_probe = Arc::downgrade(&probe);
    drop(probe);
    tokio::time::timeout(Duration::from_secs(5), async {
        while weak_probe.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(bootstrap);
    context
        .shutdown_tasks(Duration::from_secs(5))
        .await
        .assert_no_task_leak()
        .unwrap();
    assert!(telemetry.shutdown().metrics_shutdown_ok);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collection_in_progress_allows_real_route_shutdown_and_pending_cleanup() {
    let fixture = fixture(2, Duration::from_millis(2));
    for header in &fixture.headers[..16] {
        assert!(fixture.service.submit(header.clone()));
    }
    fixture.probe.armed.store(true, Ordering::Release);
    assert_eq!(sample(&fixture.samples).unwrap().0, 16);
    let (ready_tx, ready_rx) = mpsc::channel();
    let producers = fixture.headers[16..]
        .chunks(4)
        .map(|headers| {
            let service = Arc::clone(&fixture.service);
            let headers = headers.to_vec();
            let ready_tx = ready_tx.clone();
            std::thread::spawn(move || {
                assert!(service.submit(headers[0].clone()));
                ready_tx.send(()).unwrap();
                for header in &headers[1..] {
                    assert!(service.submit(header.clone()));
                }
            })
        })
        .collect::<Vec<_>>();
    for _ in &producers {
        ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }
    let report = tokio::time::timeout(Duration::from_secs(5), fixture.service.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(report.is_healthy());
    for producer in producers {
        producer.join().unwrap();
    }
    assert!(fixture
        .headers
        .iter()
        .all(|header| !is_registered(&fixture.bootstrap, header)));
    for index in 0..fixture.headers.len() {
        assert!(matches!(
            fixture
                .bootstrap
                .runtime_inner()
                .route_info_manager()
                .pickup_topic_route_data(&format!("collection-topic-{index}")),
            Ok(crate::route::route_info_manager::TopicRouteLookupStatus::NotFound)
        ));
    }
    finish(fixture).await;
}

#[tokio::test(flavor = "current_thread")]
async fn collection_in_progress_allows_aborted_batch_pending_cleanup() {
    let fixture = fixture(64, Duration::from_secs(30));
    for header in &fixture.headers {
        assert!(fixture.service.submit(header.clone()));
    }
    fixture.service.start().unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while fixture.service.queue_length() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    fixture.probe.armed.store(true, Ordering::Release);
    assert_eq!(sample(&fixture.samples).unwrap().0, fixture.headers.len());
    // Keep cancellation and abort on the same executor thread so the worker
    // cannot gracefully drain between them. SDK collection remains concurrent
    // on the log exporter's PeriodicReader thread.
    match &*fixture.service.lifecycle.lock() {
        ServiceLifecycle::Running(group) => {
            group.shutdown_now();
        }
        _ => panic!("worker must be collecting the partial batch"),
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while fixture.service.queue.pending_len() != 0 || !fixture.service.queue.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(fixture
        .headers
        .iter()
        .all(|header| is_registered(&fixture.bootstrap, header)));
    finish(fixture).await;
}
