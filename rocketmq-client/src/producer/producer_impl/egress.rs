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

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::ResourceBudget;
use rocketmq_runtime::ResourcePermit;
use rocketmq_transport::api::RequestDeadline;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::runtime::ClientMetrics;

const DEFAULT_EGRESS_WORKERS: usize = 2;

pub(crate) type OnewaySendFuture = Pin<Box<dyn Future<Output = RocketMQResult<()>> + Send + 'static>>;
pub(crate) type OnewaySend =
    Box<dyn FnOnce(CheetahString, RequestDeadline, ResourcePermit) -> OnewaySendFuture + Send + 'static>;

pub(crate) struct OnewayEnvelope {
    pub(crate) broker_addr: CheetahString,
    pub(crate) deadline: RequestDeadline,
    pub(crate) send: OnewaySend,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OnewayAdmissionOutcome {
    Accepted,
    Rejected(OnewayAdmissionRejection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OnewayAdmissionRejection {
    DeadlineExpired,
    Capacity,
    Closed,
}

struct ReservedEnvelope {
    envelope: OnewayEnvelope,
    permit: ResourcePermit,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct OnewayEgressSnapshot {
    pub(crate) current_items: usize,
    pub(crate) current_bytes: usize,
    pub(crate) accepted: u64,
    pub(crate) delivered: u64,
    pub(crate) failed: u64,
    pub(crate) cancelled: u64,
    pub(crate) rejected: u64,
    pub(crate) closed: bool,
}

#[derive(Default)]
struct OnewayEgressMetrics {
    accepted: AtomicU64,
    delivered: AtomicU64,
    failed: AtomicU64,
    cancelled: AtomicU64,
    rejected: AtomicU64,
}

pub(crate) struct BoundedEgress {
    budget: ResourceBudget,
    sender: Mutex<Option<mpsc::Sender<ReservedEnvelope>>>,
    metrics: Arc<OnewayEgressMetrics>,
    client_metrics: ClientMetrics,
}

impl BoundedEgress {
    pub(crate) fn new(
        service_context: &ChildServiceContext,
        producer_group: &str,
        count_limit: usize,
        byte_limit: usize,
        tracker: &TaskTracker,
        cancellation: &CancellationToken,
        client_metrics: ClientMetrics,
    ) -> RocketMQResult<Self> {
        let process_budget = service_context.process_budget();
        let process_capacity = process_budget.limit().capacity;
        let count_limit = count_limit.min(process_capacity.count).max(1);
        let byte_limit = byte_limit.min(process_capacity.bytes).max(1);
        let producer_name = format!("producer-{}", sanitize_budget_name(producer_group));
        let producer_budget = process_budget
            .child(
                producer_name,
                BudgetLimit::new(count_limit, byte_limit, FullPolicy::Reject),
            )
            .and_then(|producer| {
                producer.child(
                    "oneway-egress",
                    BudgetLimit::new(count_limit, byte_limit, FullPolicy::Reject),
                )
            })
            .map_err(|error| RocketMQError::ConfigInvalidValue {
                key: "producer.onewayEgress",
                value: format!("{count_limit} items/{byte_limit} bytes"),
                reason: error.to_string(),
            })?;
        let (sender, receiver) = mpsc::channel(count_limit);
        let receiver = Arc::new(tokio::sync::Mutex::new(receiver));
        let metrics = Arc::new(OnewayEgressMetrics::default());

        for worker_index in 0..DEFAULT_EGRESS_WORKERS {
            let receiver = Arc::clone(&receiver);
            let metrics = Arc::clone(&metrics);
            let client_metrics = client_metrics.clone();
            let worker_budget = producer_budget.clone();
            let cancellation = cancellation.clone();
            let task = tracker.track_future(async move {
                run_worker(receiver, metrics, cancellation, client_metrics, worker_budget).await;
            });
            service_context
                .spawn_service(format!("producer.oneway-egress.worker-{worker_index}"), task)
                .map_err(|error| {
                    RocketMQError::response_process_failed("producer.onewayEgress.worker", error.to_string())
                })?;
        }

        Ok(Self {
            budget: producer_budget,
            sender: Mutex::new(Some(sender)),
            metrics,
            client_metrics,
        })
    }

    pub(crate) fn try_admit<F>(
        &self,
        retained_bytes: usize,
        deadline: RequestDeadline,
        build: F,
    ) -> RocketMQResult<OnewayAdmissionOutcome>
    where
        F: FnOnce() -> RocketMQResult<OnewayEnvelope>,
    {
        if deadline.is_expired() {
            return Ok(self.reject(OnewayAdmissionRejection::DeadlineExpired));
        }
        let sender = self.sender.lock();
        let Some(sender) = sender.as_ref() else {
            return Ok(self.reject(OnewayAdmissionRejection::Closed));
        };
        let Ok(permit) = self.budget.try_acquire_data(retained_bytes) else {
            return Ok(self.reject(OnewayAdmissionRejection::Capacity));
        };
        let envelope = build()?;
        if envelope.deadline.is_expired() {
            return Ok(self.reject(OnewayAdmissionRejection::DeadlineExpired));
        }
        if let Err(error) = sender.try_send(ReservedEnvelope { envelope, permit }) {
            let rejection = match error {
                mpsc::error::TrySendError::Full(_) => OnewayAdmissionRejection::Capacity,
                mpsc::error::TrySendError::Closed(_) => OnewayAdmissionRejection::Closed,
            };
            return Ok(self.reject(rejection));
        }
        self.metrics.accepted.fetch_add(1, Ordering::Relaxed);
        self.client_metrics.record_oneway_egress_event("accepted");
        self.record_state();
        Ok(OnewayAdmissionOutcome::Accepted)
    }

    pub(crate) fn close(&self) {
        self.sender.lock().take();
        self.record_state();
    }

    pub(crate) fn snapshot(&self) -> OnewayEgressSnapshot {
        let budget = self.budget.snapshot();
        OnewayEgressSnapshot {
            current_items: budget.current_count,
            current_bytes: budget.current_bytes,
            accepted: self.metrics.accepted.load(Ordering::Relaxed),
            delivered: self.metrics.delivered.load(Ordering::Relaxed),
            failed: self.metrics.failed.load(Ordering::Relaxed),
            cancelled: self.metrics.cancelled.load(Ordering::Relaxed),
            rejected: self.metrics.rejected.load(Ordering::Relaxed),
            closed: self.sender.lock().is_none(),
        }
    }

    #[cfg(test)]
    fn try_reserve(&self, retained_bytes: usize) -> Result<ResourcePermit, rocketmq_runtime::BudgetRejection> {
        self.budget.try_acquire_data(retained_bytes)
    }

    fn record_state(&self) {
        let snapshot = self.budget.snapshot();
        self.client_metrics.record_oneway_egress_state(
            u64::try_from(snapshot.current_count).unwrap_or(u64::MAX),
            u64::try_from(snapshot.current_bytes).unwrap_or(u64::MAX),
            0,
            0,
        );
    }

    fn reject(&self, rejection: OnewayAdmissionRejection) -> OnewayAdmissionOutcome {
        self.metrics.rejected.fetch_add(1, Ordering::Relaxed);
        self.client_metrics.record_oneway_egress_event("rejected");
        self.record_state();
        OnewayAdmissionOutcome::Rejected(rejection)
    }
}

async fn run_worker(
    receiver: Arc<tokio::sync::Mutex<mpsc::Receiver<ReservedEnvelope>>>,
    metrics: Arc<OnewayEgressMetrics>,
    cancellation: CancellationToken,
    client_metrics: ClientMetrics,
    budget: ResourceBudget,
) {
    loop {
        let next = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                metrics.cancelled.fetch_add(1, Ordering::Relaxed);
                client_metrics.record_oneway_egress_event("cancelled");
                return;
            }
            next = async {
                let mut receiver = receiver.lock().await;
                receiver.recv().await
            } => next,
        };
        let Some(reserved) = next else {
            return;
        };
        let OnewayEnvelope {
            broker_addr,
            deadline,
            send,
        } = reserved.envelope;
        let remote_addr = broker_addr.clone();
        let send = send(broker_addr, deadline, reserved.permit);
        tokio::pin!(send);
        let result = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                metrics.cancelled.fetch_add(1, Ordering::Relaxed);
                client_metrics.record_oneway_egress_event("cancelled");
                return;
            }
            result = &mut send => result,
        };
        match result {
            Ok(()) => {
                metrics.delivered.fetch_add(1, Ordering::Relaxed);
                client_metrics.record_oneway_egress_event("delivered");
            }
            Err(error) => {
                metrics.failed.fetch_add(1, Ordering::Relaxed);
                client_metrics.record_oneway_egress_event("failed");
                tracing::warn!(remote_addr = %remote_addr, error = ?error, "producer one-way egress send failed");
            }
        }
        let snapshot = budget.snapshot();
        client_metrics.record_oneway_egress_state(
            u64::try_from(snapshot.current_count).unwrap_or(u64::MAX),
            u64::try_from(snapshot.current_bytes).unwrap_or(u64::MAX),
            0,
            0,
        );
    }
}

fn sanitize_budget_name(name: &str) -> String {
    let sanitized = name
        .chars()
        .map(|character| if character == '/' { '_' } else { character })
        .collect::<String>();
    if sanitized.trim().is_empty() {
        "default".to_string()
    } else {
        sanitized
    }
}

#[cfg(test)]
mod tests {
    use std::panic::AssertUnwindSafe;
    use std::time::Duration;

    use rocketmq_runtime::ProcessMemoryLimit;
    use rocketmq_runtime::RuntimeConfig;
    use rocketmq_runtime::RuntimeOwner;

    use super::*;

    fn fixture() -> (RuntimeOwner, BoundedEgress, TaskTracker, CancellationToken) {
        let owner = RuntimeOwner::plan(RuntimeConfig::default())
            .expect("default runtime configuration is valid")
            .with_memory_limit(ProcessMemoryLimit::configured(64).expect("memory limit"))
            .build()
            .expect("runtime owner");
        let tracker = TaskTracker::new();
        let cancellation = CancellationToken::new();
        let egress = BoundedEgress::new(
            &owner.root_context().component("egress-test"),
            "test",
            1,
            64,
            &tracker,
            &cancellation,
            ClientMetrics::noop(),
        )
        .expect("egress");
        (owner, egress, tracker, cancellation)
    }

    #[test]
    fn overload_rejects_before_materializing_another_envelope() {
        let (_owner, egress, _tracker, _cancellation) = fixture();
        let permit = egress.try_reserve(64).expect("fill byte budget");
        let built = AtomicU64::new(0);
        let result = egress.try_admit(1, RequestDeadline::after(Duration::from_secs(1)), || {
            built.fetch_add(1, Ordering::Relaxed);
            unreachable!("overload must reject before building")
        });

        assert_eq!(
            result.expect("capacity is a normal rejection"),
            OnewayAdmissionOutcome::Rejected(OnewayAdmissionRejection::Capacity)
        );
        assert_eq!(built.load(Ordering::Relaxed), 0);
        assert_eq!(egress.snapshot().rejected, 1);
        drop(permit);
    }

    #[test]
    fn closed_and_expired_admission_do_not_run_the_builder() {
        let (_owner, egress, _tracker, _cancellation) = fixture();
        egress.close();
        let closed = egress.try_admit(1, RequestDeadline::after(Duration::from_secs(1)), || {
            unreachable!("closed egress must reject before building")
        });
        assert_eq!(
            closed.expect("closed admission is a normal rejection"),
            OnewayAdmissionOutcome::Rejected(OnewayAdmissionRejection::Closed)
        );
        assert_eq!(egress.snapshot().rejected, 1);

        let (_owner, open, _tracker, _cancellation) = fixture();
        let expired = open.try_admit(1, RequestDeadline::from_timeout_millis(0), || {
            unreachable!("expired egress admission must reject before building")
        });
        assert_eq!(
            expired.expect("expired admission is a normal rejection"),
            OnewayAdmissionOutcome::Rejected(OnewayAdmissionRejection::DeadlineExpired)
        );
        assert_eq!(open.snapshot().rejected, 1);
    }

    #[test]
    fn builder_panic_releases_the_process_reservation() {
        let (_owner, egress, _tracker, _cancellation) = fixture();
        let panic = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _ = egress.try_admit(32, RequestDeadline::after(Duration::from_secs(1)), || {
                panic!("synthetic builder panic")
            });
        }));

        assert!(panic.is_err());
        assert_eq!(egress.snapshot().current_bytes, 0);
    }

    #[test]
    fn close_joins_the_fixed_worker_set() {
        let (owner, egress, tracker, _cancellation) = fixture();
        assert_eq!(tracker.len(), DEFAULT_EGRESS_WORKERS);
        egress.close();
        tracker.close();

        owner
            .block_on(async { tokio::time::timeout(Duration::from_secs(1), tracker.wait()).await })
            .expect("fixed egress workers must drain after admission closes");
        assert!(egress.snapshot().closed);
    }
}
