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

use super::*;

#[derive(Clone, Copy, Debug)]
pub(super) struct LifecycleEventConfig {
    pub(super) queue_capacity: usize,
    pub(super) publish_timeout: Duration,
    pub(super) drain_timeout: Duration,
    pub(super) listener_callback_budget: Duration,
}

impl Default for LifecycleEventConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 1024,
            publish_timeout: Duration::from_millis(10),
            drain_timeout: Duration::from_millis(250),
            listener_callback_budget: Duration::from_millis(50),
        }
    }
}

impl LifecycleEventConfig {
    pub(super) fn validate(self) -> RocketMQResult<Self> {
        validate_positive_config("channelEventQueueCapacity", self.queue_capacity)?;
        validate_duration_config("channelEventPublishTimeoutMillis", self.publish_timeout)?;
        validate_duration_config("channelEventDrainTimeoutMillis", self.drain_timeout)?;
        validate_duration_config(
            "channelEventListenerCallbackBudgetMillis",
            self.listener_callback_budget,
        )?;
        Ok(self)
    }
}

fn validate_positive_config(
    key: &'static str,
    value: impl TryInto<u64> + Copy + std::fmt::Display,
) -> RocketMQResult<()> {
    if value.try_into().ok().is_some_and(|value| value > 0) {
        return Ok(());
    }
    Err(RocketMQError::ConfigInvalidValue {
        key,
        value: value.to_string(),
        reason: "must be greater than zero".to_owned(),
    })
}

fn validate_duration_config(key: &'static str, value: Duration) -> RocketMQResult<()> {
    if !value.is_zero() {
        return Ok(());
    }
    Err(RocketMQError::ConfigInvalidValue {
        key,
        value: format!("{value:?}"),
        reason: "must be greater than zero".to_owned(),
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
pub(super) enum LifecycleEventPublishOutcome {
    Queued,
    DeadlineExpired,
    DispatcherClosed,
    ShuttingDown,
}

impl LifecycleEventPublishOutcome {
    const fn metric_label(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::DeadlineExpired => "deadline_expired",
            Self::DispatcherClosed => "dropped_dispatcher_closed",
            Self::ShuttingDown => "dropped_shutdown",
        }
    }

    pub(super) const fn is_queued(self) -> bool {
        matches!(self, Self::Queued)
    }
}

#[derive(Clone)]
pub(super) struct LifecycleEventPublisher {
    pub(super) sender: mpsc::Sender<TokioEvent>,
    pub(super) publish_timeout: Duration,
    pub(super) cancellation: CancellationToken,
    pub(super) telemetry: TransportTelemetry,
}

impl LifecycleEventPublisher {
    pub(super) async fn publish(&self, event: TokioEvent) -> LifecycleEventPublishOutcome {
        let event_name = lifecycle_event_name(event.type_());
        let outcome = enqueue_lifecycle_event(&self.sender, event, self.publish_timeout, &self.cancellation).await;
        self.telemetry
            .record_lifecycle_event(event_name, outcome.metric_label());
        outcome
    }
}

pub(super) async fn enqueue_lifecycle_event<T>(
    sender: &mpsc::Sender<T>,
    event: T,
    publish_timeout: Duration,
    cancellation: &CancellationToken,
) -> LifecycleEventPublishOutcome {
    if cancellation.is_cancelled() {
        return LifecycleEventPublishOutcome::ShuttingDown;
    }

    tokio::select! {
        biased;
        _ = cancellation.cancelled() => LifecycleEventPublishOutcome::ShuttingDown,
        result = tokio::time::timeout(publish_timeout, sender.send(event)) => match result {
            Ok(Ok(())) => LifecycleEventPublishOutcome::Queued,
            Ok(Err(_)) => LifecycleEventPublishOutcome::DispatcherClosed,
            Err(_) => LifecycleEventPublishOutcome::DeadlineExpired,
        },
    }
}

const fn lifecycle_event_name(event: &ConnectionNetEvent) -> &'static str {
    match event {
        ConnectionNetEvent::CONNECTED(_) => "connected",
        ConnectionNetEvent::DISCONNECTED => "disconnected",
        ConnectionNetEvent::EXCEPTION => "exception",
        ConnectionNetEvent::IDLE => "idle",
    }
}

pub(super) async fn run_lifecycle_event_dispatcher(
    mut receiver: mpsc::Receiver<TokioEvent>,
    listener: Arc<dyn ChannelEventListener>,
    cancellation: CancellationToken,
    config: LifecycleEventConfig,
    listener_blocking: BlockingExecutor,
    telemetry: TransportTelemetry,
) {
    loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            event = receiver.recv() => match event {
                Some(event) => dispatch_lifecycle_event(
                    Arc::clone(&listener),
                    event,
                    ShutdownDeadline::after(config.listener_callback_budget),
                    &listener_blocking,
                    &telemetry,
                ).await,
                None => {
                    info!("Remoting lifecycle event dispatcher closed");
                    return;
                }
            },
        }
    }

    let drain_deadline = Instant::now() + config.drain_timeout;
    while Instant::now() < drain_deadline {
        let Ok(event) = receiver.try_recv() else {
            break;
        };
        let callback_deadline = Instant::now()
            .checked_add(config.listener_callback_budget)
            .unwrap_or(drain_deadline)
            .min(drain_deadline);
        dispatch_lifecycle_event(
            Arc::clone(&listener),
            event,
            ShutdownDeadline::at(callback_deadline),
            &listener_blocking,
            &telemetry,
        )
        .await;
    }

    let dropped = receiver.len();
    for _ in 0..dropped {
        telemetry.record_lifecycle_event("pending", "dropped_drain_deadline");
    }
    if dropped > 0 {
        warn!(dropped, "Remoting lifecycle event drain deadline expired");
    }
    receiver.close();
    info!("Remoting lifecycle event dispatcher terminated");
}

async fn dispatch_lifecycle_event(
    listener: Arc<dyn ChannelEventListener>,
    event: TokioEvent,
    deadline: ShutdownDeadline,
    listener_blocking: &BlockingExecutor,
    telemetry: &TransportTelemetry,
) {
    let event_name = lifecycle_event_name(event.type_());
    let wait_started = Instant::now();
    let callback_telemetry = telemetry.clone();
    let outcome = listener_blocking
        .spawn_io_until("rocketmq.remoting.lifecycle_listener", deadline, move || {
            let callback_started = Instant::now();
            let addr = event.remote_addr().to_string();
            match event.type_() {
                ConnectionNetEvent::CONNECTED(_) => listener.on_channel_connect(&addr, event.channel()),
                ConnectionNetEvent::DISCONNECTED => listener.on_channel_close(&addr, event.channel()),
                ConnectionNetEvent::EXCEPTION => listener.on_channel_exception(&addr, event.channel()),
                ConnectionNetEvent::IDLE => listener.on_channel_idle(&addr, event.channel()),
            }
            callback_telemetry.record_lifecycle_listener_latency(callback_started.elapsed(), event_name);
        })
        .await;
    let wait_elapsed = wait_started.elapsed();
    let outcome_label = match outcome {
        Ok(()) => "delivered",
        Err(rocketmq_runtime::RuntimeError::BlockingTaskTimeoutStillRunning { .. }) => "callback_timeout_still_running",
        Err(
            rocketmq_runtime::RuntimeError::BlockingQueueTimeout { .. }
            | rocketmq_runtime::RuntimeError::BlockingQueueFull { .. },
        ) => "executor_deadline_or_rejected",
        Err(rocketmq_runtime::RuntimeError::BlockingJoin { .. }) => "callback_join_failure",
        Err(error) => {
            warn!(
                event = event_name,
                elapsed_ms = wait_elapsed.as_millis(),
                cause = %error,
                "Remoting lifecycle listener executor failed"
            );
            "executor_failure"
        }
    };
    telemetry.record_lifecycle_event(event_name, outcome_label);
    if outcome_label != "delivered" {
        warn!(
            event = event_name,
            outcome = outcome_label,
            elapsed_ms = wait_elapsed.as_millis(),
            "Remoting lifecycle listener callback was not delivered within its deadline"
        );
    }
}
