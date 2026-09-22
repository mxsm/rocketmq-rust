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
use std::time::Duration;

use rocketmq_runtime::{
    ChildServiceContext, RuntimeComponent, RuntimeDiagnosticsInputs, ShutdownDeadline, ShutdownReport,
};
use tokio::sync::OnceCell;

use super::{read_token, serve, RuntimeDiagnosticsEndpointConfig, RuntimeDiagnosticsEndpointHandle};
use crate::metrics::runtime::RuntimeMetricsRecorder;
use crate::{ObservabilityError, TelemetryHandle};

pub const RUNTIME_DIAGNOSTICS_MODE_ENV: &str = "ROCKETMQ_RUNTIME_DIAGNOSTICS_MODE";

/// Explicitly selects diagnostic work, independently of compiled features.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeDiagnosticsMode {
    Disabled,
    MetricsOnly { sample_interval: Duration },
    EndpointOnly(RuntimeDiagnosticsEndpointConfig),
    EndpointAndMetrics(RuntimeDiagnosticsEndpointConfig),
}

impl RuntimeDiagnosticsMode {
    /// Loads explicit mode intent, retaining the legacy endpoint opt-in default.
    ///
    /// With no mode configured, an endpoint configuration selects both outputs;
    /// no endpoint configuration selects `Disabled`. A no-op telemetry handle
    /// never creates a sampler, including in either metrics-requesting mode.
    ///
    /// # Errors
    ///
    /// Returns a sanitized configuration error for an unknown mode or invalid
    /// settings required by that mode.
    pub fn from_env() -> Result<Self, ObservabilityError> {
        match super::optional_env(RUNTIME_DIAGNOSTICS_MODE_ENV)?.as_deref() {
            Some("disabled") => Ok(Self::Disabled),
            Some("metrics_only") => Ok(Self::MetricsOnly {
                sample_interval: super::parse_sample_interval()?,
            }),
            Some(mode @ ("endpoint_only" | "endpoint_and_metrics")) => {
                let config = RuntimeDiagnosticsEndpointConfig::from_env()?.ok_or_else(|| {
                    ObservabilityError::invalid_config(
                        "runtime diagnostics endpoint mode requires bind and token settings",
                    )
                })?;
                Ok(if mode == "endpoint_only" {
                    Self::EndpointOnly(config)
                } else {
                    Self::EndpointAndMetrics(config)
                })
            }
            Some(_) => Err(ObservabilityError::invalid_config("unknown runtime diagnostics mode")),
            None => Ok(RuntimeDiagnosticsEndpointConfig::from_env()?.map_or(Self::Disabled, Self::EndpointAndMetrics)),
        }
    }

    fn endpoint(&self) -> Option<&RuntimeDiagnosticsEndpointConfig> {
        match self {
            Self::EndpointOnly(config) | Self::EndpointAndMetrics(config) => Some(config),
            Self::Disabled | Self::MetricsOnly { .. } => None,
        }
    }

    fn sample_interval(&self) -> Option<Duration> {
        match self {
            Self::MetricsOnly { sample_interval } => Some(*sample_interval),
            Self::EndpointAndMetrics(config) => Some(config.sample_interval),
            Self::Disabled | Self::EndpointOnly(_) => None,
        }
    }
}

/// Bounded, synchronous access to diagnostics owned by the business component.
///
/// Implementations must read in-memory state without disk/network I/O, waiting
/// for business completion, or unbounded scans. Retained data must not keep write
/// authority alive after component shutdown. V2 keeps its existing convention:
/// an empty schedule vector is omitted, including when it represents known zero.
pub trait RuntimeDiagnosticsDataProvider: Send + Sync {
    fn snapshot(&self) -> RuntimeDiagnosticsInputs;
}

impl<F> RuntimeDiagnosticsDataProvider for F
where
    F: Fn() -> RuntimeDiagnosticsInputs + Send + Sync,
{
    fn snapshot(&self) -> RuntimeDiagnosticsInputs {
        self()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeDiagnosticsStarted {
    pub endpoint: Option<RuntimeDiagnosticsEndpointHandle>,
    pub sampler_active: bool,
}

struct StartedConfiguration {
    mode: RuntimeDiagnosticsMode,
    handle: RuntimeDiagnosticsStarted,
}

struct ServiceInner {
    context: ChildServiceContext,
    observed: ChildServiceContext,
    component: RuntimeComponent,
    telemetry: TelemetryHandle,
    provider: Arc<dyn RuntimeDiagnosticsDataProvider>,
    started: OnceCell<StartedConfiguration>,
}

/// One owned, idempotently initialized sampler and optional HTTP listener.
///
/// Clones share initialization. Keep one instance at the composition root;
/// constructing another service creates a separate owner. Dropping this handle
/// does not detach its tasks from the supplied parent. Shutdown cancels and
/// awaits both outputs using the caller's deadline.
#[derive(Clone)]
pub struct RuntimeDiagnosticsService {
    inner: Arc<ServiceInner>,
}

impl RuntimeDiagnosticsService {
    pub fn new(
        parent: &ChildServiceContext,
        component: RuntimeComponent,
        telemetry: TelemetryHandle,
        provider: Arc<dyn RuntimeDiagnosticsDataProvider>,
    ) -> Self {
        Self {
            inner: Arc::new(ServiceInner {
                context: parent.component("runtime-diagnostics"),
                observed: parent.clone(),
                component,
                telemetry,
                provider,
                started: OnceCell::new(),
            }),
        }
    }

    /// Starts exactly the requested outputs for this service instance.
    ///
    /// # Errors
    ///
    /// Returns a sanitized error for invalid sampling bounds, endpoint setup,
    /// closed ownership, or reinitialization with a different configuration.
    pub async fn start(&self, mode: RuntimeDiagnosticsMode) -> Result<RuntimeDiagnosticsStarted, ObservabilityError> {
        if self.inner.context.task_group().lifecycle_state() != rocketmq_runtime::TaskGroupLifecycleState::Open {
            return Err(ObservabilityError::invalid_config(
                "runtime diagnostics owner is closed",
            ));
        }
        if mode
            .sample_interval()
            .is_some_and(|interval| interval.is_zero() || interval > Duration::from_secs(300))
        {
            return Err(ObservabilityError::invalid_config(
                "runtime diagnostics sample interval must be positive and at most 300 seconds",
            ));
        }
        let started = self
            .inner
            .started
            .get_or_try_init(|| self.initialize(mode.clone()))
            .await?;
        if started.mode != mode {
            return Err(ObservabilityError::invalid_config(
                "runtime diagnostics service already has a different configuration",
            ));
        }
        Ok(started.handle)
    }

    async fn initialize(&self, mode: RuntimeDiagnosticsMode) -> Result<StartedConfiguration, ObservabilityError> {
        let context = &self.inner.context;
        let bound = if let Some(config) = mode.endpoint() {
            read_token(context, config.token_file.clone()).await?;
            let listener = tokio::net::TcpListener::bind(config.bind_addr)
                .await
                .map_err(|_| ObservabilityError::invalid_config("runtime diagnostics listener cannot bind"))?;
            let local_addr = listener.local_addr().map_err(|_| {
                ObservabilityError::invalid_config("runtime diagnostics listener address is unavailable")
            })?;
            Some((listener, local_addr, config.token_file.clone()))
        } else {
            None
        };
        let recorder = if mode.sample_interval().is_some() {
            RuntimeMetricsRecorder::from_handle(&self.inner.telemetry, self.inner.component)
        } else {
            RuntimeMetricsRecorder::noop(self.inner.component)
        };
        let sample_interval = mode.sample_interval().filter(|_| recorder.is_enabled());
        let sampler_active = sample_interval.is_some();
        if let Some(sample_interval) = sample_interval {
            let sample_context = self.inner.observed.clone();
            let component = self.inner.component;
            let provider = self.inner.provider.clone();
            let metrics = recorder.clone();
            context
                .scheduled_tasks("sampler")
                .schedule_bounded(
                    rocketmq_runtime::ScheduledTaskConfig::fixed_delay("runtime-diagnostics.sample", sample_interval),
                    rocketmq_runtime::ScheduledExecutionPolicy::serial(rocketmq_runtime::MissedTickPolicy::Skip),
                    move || {
                        let context = sample_context.clone();
                        let provider = provider.clone();
                        let metrics = metrics.clone();
                        async move {
                            metrics.record_snapshot_v2(&context.diagnostics_view_v2(component, provider.snapshot()));
                        }
                    },
                )
                .map_err(|_| ObservabilityError::invalid_config("runtime diagnostics sampler cannot start"))?;
        }
        let endpoint = if let Some((listener, local_addr, token_file)) = bound {
            let cancellation = context.task_group().cancellation_token();
            let endpoint_context = super::EndpointContext {
                owner: context.clone(),
                observed: self.inner.observed.clone(),
                component: self.inner.component,
                token_file,
                metrics: recorder,
                provider: self.inner.provider.clone(),
            };
            if context
                .spawn_service("runtime-diagnostics.endpoint", async move {
                    serve(listener, endpoint_context, cancellation).await;
                })
                .is_err()
            {
                context.task_group().shutdown(Duration::ZERO).await;
                return Err(ObservabilityError::invalid_config(
                    "runtime diagnostics endpoint cannot start",
                ));
            }
            Some(RuntimeDiagnosticsEndpointHandle { local_addr })
        } else {
            None
        };
        Ok(StartedConfiguration {
            mode,
            handle: RuntimeDiagnosticsStarted {
                endpoint,
                sampler_active,
            },
        })
    }

    pub async fn shutdown_until(&self, deadline: ShutdownDeadline) -> ShutdownReport {
        self.inner.context.task_group().shutdown_until(deadline).await
    }
}
