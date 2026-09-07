// Copyright 2025 The RocketMQ Rust Authors
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

//! RocketMQ Dashboard - Modern GUI for Apache RocketMQ monitoring
//!
//! This application provides a real-time dashboard for monitoring
//! and managing RocketMQ clusters, topics, and message flows.

mod app;
mod assets;
mod components;
mod features;
mod infrastructure;
mod route;
mod services;
mod state;
mod theme;
mod ui;

use std::{error::Error as StdError, fmt, process::ExitCode, sync::Arc};

use app::RocketmqDashboard;
use gpui::*;
use gpui_component::Root;
use infrastructure::{
    admin_provider::GpuiAdminProvider, auth_state::DesktopAuthState, client_runtime::DesktopClientRuntime,
    config_store::DesktopConfigStore,
};
use services::AppServices;
use tracing::{error, info};

const SMOKE_EXIT_ENV: &str = "ROCKETMQ_DASHBOARD_GPUI_SMOKE_EXIT";
const SMOKE_WIDTH_ENV: &str = "ROCKETMQ_DASHBOARD_GPUI_SMOKE_WIDTH";

#[derive(Clone, Copy, Debug)]
enum AppErrorCode {
    Observability,
    RuntimeStartup,
    Configuration,
    Window,
    Shutdown,
}

struct AppError {
    code: AppErrorCode,
    sources: Vec<Arc<dyn StdError + Send + Sync>>,
}

impl AppError {
    fn new(code: AppErrorCode) -> Self {
        Self {
            code,
            sources: Vec::new(),
        }
    }

    fn caused_by(code: AppErrorCode, source: impl StdError + Send + Sync + 'static) -> Self {
        Self {
            code,
            sources: vec![Arc::new(source)],
        }
    }

    fn push_source(&mut self, source: impl StdError + Send + Sync + 'static) {
        self.sources.push(Arc::new(source));
    }

    fn caused_by_boxed(code: AppErrorCode, source: Box<dyn StdError + Send + Sync>) -> Self {
        Self {
            code,
            sources: vec![Arc::from(source)],
        }
    }

    fn code(&self) -> &'static str {
        match self.code {
            AppErrorCode::Observability => "RGPUI-OBSERVABILITY",
            AppErrorCode::RuntimeStartup => "RGPUI-RUNTIME-STARTUP",
            AppErrorCode::Configuration => "RGPUI-CONFIGURATION",
            AppErrorCode::Window => "RGPUI-WINDOW",
            AppErrorCode::Shutdown => "RGPUI-SHUTDOWN",
        }
    }
}

impl fmt::Debug for AppError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AppError")
            .field("code", &self.code)
            .field("source_count", &self.sources.len())
            .finish()
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.code {
            AppErrorCode::Observability => "Dashboard observability could not start.",
            AppErrorCode::RuntimeStartup => "Dashboard runtime could not start.",
            AppErrorCode::Configuration => "Dashboard configuration could not be initialized.",
            AppErrorCode::Window => "Dashboard window could not be created.",
            AppErrorCode::Shutdown => "Dashboard shutdown did not complete cleanly.",
        })
    }
}

impl StdError for AppError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.sources.first().map(|source| source.as_ref() as _)
    }
}

fn initial_window_width() -> f32 {
    std::env::var(SMOKE_WIDTH_ENV)
        .ok()
        .and_then(|value| value.parse::<f32>().ok())
        .filter(|width| width.is_finite() && *width >= 640.0)
        .unwrap_or(1440.0)
}

/// Main entry point for the RocketMQ Dashboard application.
fn main() -> ExitCode {
    terminate(run())
}

fn terminate(result: Result<(), AppError>) -> ExitCode {
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("RocketMQ Dashboard failed [{}]: {error}", error.code());
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), AppError> {
    let environment_filter = rocketmq_observability::read_rust_log()
        .map_err(|source| AppError::caused_by(AppErrorCode::Observability, source))?;
    let resolved_filter = rocketmq_observability::LogFilterResolver::resolve(rocketmq_observability::LogFilterInputs {
        environment: environment_filter.as_deref(),
        ..rocketmq_observability::LogFilterInputs::default()
    })
    .map_err(|source| AppError::caused_by(AppErrorCode::Observability, source))?;
    let mut bootstrap = rocketmq_observability::TelemetryBootstrapConfig::default();
    bootstrap.observability.service_name = "rocketmq-dashboard-gpui".to_string();
    bootstrap.observability.service_namespace = "rocketmq".to_string();
    bootstrap.observability.node_type = "dashboard".to_string();
    bootstrap.observability.node_id = "gpui".to_string();
    bootstrap.observability.subscriber_install_policy = rocketmq_observability::SubscriberInstallPolicy::Required;
    let telemetry_guard = rocketmq_observability::install_global_with_filter(&bootstrap, resolved_filter.clone())
        .map_err(|source| AppError::caused_by(AppErrorCode::Observability, source))?;
    info!(
        service = "rocketmq-dashboard-gpui",
        effective_filter = resolved_filter.filter(),
        filter_source = %resolved_filter.source(),
        subscriber_installed = telemetry_guard.subscriber_install_status().installed,
        reload_enabled = bootstrap.logging.reload.enabled,
        "GPUI telemetry bootstrap initialized"
    );

    info!("Starting RocketMQ Dashboard");

    let runtime = match DesktopClientRuntime::new(telemetry_guard.handle()) {
        Ok(runtime) => runtime,
        Err(source) => {
            let mut error = AppError::caused_by(AppErrorCode::RuntimeStartup, source);
            if let Err(source) = telemetry_guard.shutdown().into_result() {
                error.push_source(source);
            }
            return Err(error);
        }
    };
    let auth = DesktopAuthState::from_process_environment();
    let config_store = match DesktopConfigStore::from_environment(runtime.component("config-store")) {
        Ok(store) => store,
        Err(failure) => {
            let mut error = match failure.into_operational() {
                Some(source) => AppError::caused_by(AppErrorCode::Configuration, source),
                None => AppError::new(AppErrorCode::Configuration),
            };
            if let Err(source) = runtime.shutdown(None) {
                error.push_source(source);
            }
            if let Err(source) = telemetry_guard.shutdown().into_result() {
                error.push_source(source);
            }
            return Err(error);
        }
    };
    let provider = GpuiAdminProvider::new(
        runtime.provider_component("admin-provider"),
        runtime.client_runtime(),
        Arc::clone(&auth),
    );
    let services = AppServices::desktop(
        config_store,
        Arc::clone(&provider),
        auth,
        runtime.component("services"),
        runtime.component("history"),
        runtime.component("monitor"),
    );

    let app = Application::new().with_assets(assets::component_assets());
    let initial_width = initial_window_width();
    let window_error = Arc::new(parking_lot::Mutex::new(None));
    let window_error_for_run = Arc::clone(&window_error);

    app.run(move |cx| {
        // This must be called before using any GPUI Component features.
        gpui_component::init(cx);
        features::brokers::init(cx);
        theme::apply_dark_theme(cx);

        if let Err(error) = cx.open_window(
            WindowOptions {
                window_bounds: Some(WindowBounds::Windowed(Bounds {
                    origin: Point {
                        x: px(100.0),
                        y: px(100.0),
                    },
                    size: gpui::Size {
                        width: px(initial_width),
                        height: px(900.0),
                    },
                })),
                titlebar: Some(TitlebarOptions {
                    title: Some("RocketMQ Dashboard".into()),
                    appears_transparent: false,
                    traffic_light_position: None,
                }),
                ..Default::default()
            },
            |window, cx| {
                let view = cx.new(|cx| RocketmqDashboard::with_services(window, services.clone(), cx));
                // This first level on the window, should be a Root.
                cx.new(|cx| Root::new(view, window, cx))
            },
        ) {
            error!(
                error_code = "RGPUI-WINDOW",
                "Unable to create the RocketMQ Dashboard window"
            );
            *window_error_for_run.lock() = Some(AppError::caused_by_boxed(
                AppErrorCode::Window,
                error.into_boxed_dyn_error(),
            ));
            cx.quit();
        }
    });
    let runtime_shutdown = runtime.shutdown(Some(provider));
    match &runtime_shutdown {
        Ok(report) => info!(
            leaked = report.leaked,
            timed_out = report.timed_out,
            "GPUI runtime shutdown completed"
        ),
        Err(_) => error!(error_code = "RGPUI-SHUTDOWN", "GPUI runtime shutdown failed"),
    }
    let telemetry_shutdown = telemetry_guard.shutdown().into_result();
    let mut failure = window_error.lock().take();
    if let Err(source) = runtime_shutdown {
        match failure.as_mut() {
            Some(error) => error.push_source(source),
            None => failure = Some(AppError::caused_by(AppErrorCode::Shutdown, source)),
        }
    }
    if let Err(source) = telemetry_shutdown {
        match failure.as_mut() {
            Some(error) => error.push_source(source),
            None => failure = Some(AppError::caused_by(AppErrorCode::Shutdown, source)),
        }
    }
    failure.map_or(Ok(()), Err)
}

#[cfg(test)]
mod process_error_tests {
    use std::process::ExitCode;

    use super::{AppError, AppErrorCode, terminate};

    #[derive(Debug, thiserror::Error)]
    #[error("secret-value from a typed source")]
    struct SensitiveSource;

    #[test]
    fn process_projection_is_fixed_and_retains_all_typed_failures() {
        let mut error = AppError::caused_by(AppErrorCode::Window, SensitiveSource);
        error.push_source(std::io::Error::other("second-secret"));

        assert_eq!(error.code(), "RGPUI-WINDOW");
        assert_eq!(error.to_string(), "Dashboard window could not be created.");
        assert!(!format!("{error:?}").contains("secret-value"));
        assert_eq!(error.sources.len(), 2);
        assert!(std::error::Error::source(&error).is_some());
    }

    #[test]
    fn process_result_maps_success_and_failure_to_explicit_exit_codes() {
        assert_eq!(terminate(Ok(())), ExitCode::SUCCESS);
        assert_eq!(
            terminate(Err(AppError::caused_by(AppErrorCode::Shutdown, SensitiveSource))),
            ExitCode::FAILURE
        );
    }
}
