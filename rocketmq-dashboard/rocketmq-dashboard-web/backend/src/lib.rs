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
#![recursion_limit = "256"]

pub mod admin;
mod api;
pub mod config;
pub mod error;
pub mod middleware;
pub mod model;
pub mod persistence;
pub mod service;
pub mod state;

use crate::api::build_router;
use crate::config::AppConfig;
use crate::state::AppState;
use rocketmq_admin_core::client_adapter::ClientRuntime;
use rocketmq_admin_core::client_adapter::ClientRuntimeConfig;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ShutdownReport;
use std::error::Error as StdError;
use std::fmt;
use std::future::IntoFuture;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::Instant;

const APPLICATION_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

type ProcessSource = Box<dyn StdError + Send + Sync + 'static>;

#[derive(Debug)]
struct ProcessError {
    code: &'static str,
    message: &'static str,
    source: Option<ProcessSource>,
}

impl ProcessError {
    fn new<E>(code: &'static str, message: &'static str, source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self {
            code,
            message,
            source: Some(Box::new(source)),
        }
    }

    const fn fixed(code: &'static str, message: &'static str) -> Self {
        Self {
            code,
            message,
            source: None,
        }
    }
}

impl fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl StdError for ProcessError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source.as_deref().map(|source| source as &(dyn StdError + 'static))
    }
}

pub fn process_main() -> ExitCode {
    match run_process() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{}: {}", error.code, error.message);
            ExitCode::FAILURE
        }
    }
}

fn run_process() -> Result<(), ProcessError> {
    let config = AppConfig::load().map_err(|source| {
        ProcessError::new("DASHBOARD_CONFIG_INVALID", "Dashboard configuration is invalid", source)
    })?;
    let environment_filter = rocketmq_observability::read_rust_log().map_err(|source| {
        ProcessError::new(
            "LOG_FILTER_READ_FAILED",
            "Logging configuration could not be read",
            source,
        )
    })?;
    let resolved_filter = rocketmq_observability::LogFilterResolver::resolve(rocketmq_observability::LogFilterInputs {
        environment: environment_filter.as_deref(),
        ..rocketmq_observability::LogFilterInputs::default()
    })
    .map_err(|source| ProcessError::new("LOG_FILTER_INVALID", "Logging configuration is invalid", source))?;
    let owner = RuntimeOwner::plan(RuntimeConfig::server_default("rocketmq-dashboard-web-backend"))
        .map_err(|source| ProcessError::new("RUNTIME_PLAN_FAILED", "Runtime configuration is invalid", source))?
        .build()
        .map_err(|source| {
            ProcessError::new("RUNTIME_START_FAILED", "Dashboard runtime could not be started", source)
        })?;

    let mut bootstrap = rocketmq_observability::TelemetryBootstrapConfig::default();
    bootstrap.observability.service_name = "rocketmq-dashboard-web-backend".to_string();
    bootstrap.observability.service_namespace = "rocketmq".to_string();
    bootstrap.observability.node_type = "dashboard".to_string();
    bootstrap.observability.node_id = "web-backend".to_string();
    bootstrap.observability.subscriber_install_policy = rocketmq_observability::SubscriberInstallPolicy::Required;
    let telemetry_guard = match rocketmq_observability::install_global_with_filter(&bootstrap, resolved_filter.clone())
    {
        Ok(guard) => guard,
        Err(source) => {
            let error = ProcessError::new(
                "TELEMETRY_START_FAILED",
                "Dashboard telemetry could not be initialized",
                source,
            );
            report_runtime_cleanup(
                owner.shutdown_runtime_blocking(),
                "RUNTIME_SHUTDOWN_FAILED",
                "Dashboard runtime cleanup failed",
            );
            return Err(error);
        }
    };
    let client_runtime = match ClientRuntime::try_new(
        owner.root_context().component("rocketmq-admin-client"),
        ClientRuntimeConfig::default(),
        telemetry_guard.handle(),
    ) {
        Ok(runtime) => runtime,
        Err(source) => {
            let error = ProcessError::new(
                "ADMIN_RUNTIME_START_FAILED",
                "Dashboard admin runtime could not be initialized",
                source,
            );
            report_cleanup(
                telemetry_guard.shutdown().into_result(),
                "TELEMETRY_SHUTDOWN_FAILED",
                "Dashboard telemetry cleanup failed",
            );
            report_runtime_cleanup(
                owner.shutdown_runtime_blocking(),
                "RUNTIME_SHUTDOWN_FAILED",
                "Dashboard runtime cleanup failed",
            );
            return Err(error);
        }
    };
    tracing::info!(
        service = "rocketmq-dashboard-web-backend",
        effective_filter = resolved_filter.filter(),
        filter_source = %resolved_filter.source(),
        subscriber_installed = telemetry_guard.subscriber_install_status().installed,
        reload_enabled = bootstrap.logging.reload.enabled,
        "Dashboard Web telemetry bootstrap initialized"
    );

    let (run_result, client_shutdown_report) = owner.block_on(async {
        let result = Box::pin(run_with_telemetry(
            config,
            client_runtime.clone(),
            telemetry_guard.handle(),
        ))
        .await;
        let shutdown_report = client_runtime.shutdown().await;
        (result, shutdown_report)
    });
    let client_shutdown_result = require_healthy_shutdown(
        client_shutdown_report,
        "ADMIN_RUNTIME_SHUTDOWN_INCOMPLETE",
        "Dashboard admin runtime cleanup was incomplete",
    );
    let telemetry_shutdown_result = telemetry_guard.shutdown().into_result().map_err(|source| {
        ProcessError::new(
            "TELEMETRY_SHUTDOWN_FAILED",
            "Dashboard telemetry cleanup failed",
            source,
        )
    });
    let runtime_shutdown_result = owner
        .shutdown_runtime_blocking()
        .map_err(|source| ProcessError::new("RUNTIME_SHUTDOWN_FAILED", "Dashboard runtime cleanup failed", source))
        .and_then(|report| {
            require_healthy_shutdown(
                report,
                "RUNTIME_SHUTDOWN_INCOMPLETE",
                "Dashboard runtime cleanup was incomplete",
            )
        });

    run_result?;
    client_shutdown_result?;
    telemetry_shutdown_result?;
    runtime_shutdown_result?;
    Ok(())
}

fn report_cleanup<T, E>(result: Result<T, E>, code: &'static str, message: &'static str)
where
    E: StdError,
{
    if result.is_err() {
        eprintln!("{code}: {message}");
    }
}

fn report_runtime_cleanup(
    result: Result<ShutdownReport, rocketmq_runtime::RuntimeError>,
    code: &'static str,
    message: &'static str,
) {
    match result {
        Ok(report) if !report.is_healthy() => {
            report.log_if_unhealthy();
            eprintln!("{code}: {message}");
        }
        Err(_) => eprintln!("{code}: {message}"),
        Ok(_) => {}
    }
}

fn require_healthy_shutdown(
    report: ShutdownReport,
    code: &'static str,
    message: &'static str,
) -> Result<(), ProcessError> {
    if report.is_healthy() {
        Ok(())
    } else {
        report.log_if_unhealthy();
        Err(ProcessError::fixed(code, message))
    }
}

async fn run_with_telemetry(
    config: AppConfig,
    client_runtime: Arc<ClientRuntime>,
    telemetry: rocketmq_observability::TelemetryHandle,
) -> Result<(), ProcessError> {
    let addr: SocketAddr = format!("{}:{}", config.server.host, config.server.port)
        .parse()
        .map_err(|source| ProcessError::new("LISTEN_ADDRESS_INVALID", "Dashboard listen address is invalid", source))?;
    let state = AppState::try_new_with_telemetry(config, client_runtime, telemetry)
        .await
        .map_err(|source| {
            ProcessError::new(
                "DASHBOARD_STATE_START_FAILED",
                "Dashboard state could not be initialized",
                source,
            )
        })?;
    let admin_client = state.admin_client.clone();
    let app = build_router(state);
    let listener = TcpListener::bind(addr).await.map_err(|source| {
        ProcessError::new(
            "LISTENER_BIND_FAILED",
            "Dashboard listener could not be started",
            source,
        )
    })?;

    tracing::info!("RocketMQ Dashboard Web backend listening on http://{addr}");
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown_receiver.await;
        })
        .into_future();
    tokio::pin!(server);

    tokio::select! {
        server_result = server.as_mut() => {
            tokio::time::timeout(APPLICATION_SHUTDOWN_TIMEOUT, admin_client.shutdown())
                .await
                .map_err(|source| ProcessError::new("ADMIN_SHUTDOWN_TIMEOUT", "Dashboard admin cleanup timed out", source))?;
            server_result.map_err(|source| ProcessError::new("HTTP_SERVER_FAILED", "Dashboard HTTP server failed", source))?;
        }
        () = shutdown_signal() => {
            let deadline = Instant::now() + APPLICATION_SHUTDOWN_TIMEOUT;
            let _ = shutdown_sender.send(());
            let shutdown_result = tokio::time::timeout_at(deadline, async {
                let server_result = server.as_mut().await;
                admin_client.shutdown().await;
                server_result
            })
            .await;
            match shutdown_result {
                Ok(server_result) => server_result
                    .map_err(|source| ProcessError::new("HTTP_SERVER_FAILED", "Dashboard HTTP server failed", source))?,
                Err(source) => {
                    return Err(ProcessError::new(
                        "DASHBOARD_SHUTDOWN_TIMEOUT",
                        "Dashboard request draining and admin cleanup timed out",
                        source,
                    ));
                }
            }
        }
    }

    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::SignalKind;
        use tokio::signal::unix::signal;

        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    () = wait_for_ctrl_c() => {},
                    _ = terminate.recv() => {},
                }
            }
            Err(_) => {
                tracing::warn!("failed to install SIGTERM shutdown signal: using Ctrl-C only");
                wait_for_ctrl_c().await;
            }
        }
    }

    #[cfg(not(unix))]
    wait_for_ctrl_c().await;
}

async fn wait_for_ctrl_c() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::warn!(%error, "failed to install Ctrl-C shutdown signal");
        std::future::pending::<()>().await;
    }
}

#[cfg(test)]
mod process_error_tests {
    use super::require_healthy_shutdown;
    use rocketmq_runtime::ShutdownReport;
    use std::time::Duration;

    #[test]
    fn unhealthy_shutdown_report_becomes_a_fixed_process_error() {
        let mut report = ShutdownReport::new("sensitive-component-name", Duration::ZERO);
        report.failed = 1;

        let error = require_healthy_shutdown(report, "SHUTDOWN_INCOMPLETE", "Cleanup was incomplete")
            .expect_err("an unhealthy report must fail the process");

        assert_eq!(error.code, "SHUTDOWN_INCOMPLETE");
        assert_eq!(error.to_string(), "Cleanup was incomplete");
        assert!(std::error::Error::source(&error).is_none());
        assert!(!error.to_string().contains("sensitive-component-name"));
    }
}
