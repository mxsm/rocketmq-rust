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

use std::sync::Arc;

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

fn initial_window_width() -> f32 {
    std::env::var(SMOKE_WIDTH_ENV)
        .ok()
        .and_then(|value| value.parse::<f32>().ok())
        .filter(|width| width.is_finite() && *width >= 640.0)
        .unwrap_or(1440.0)
}

/// Main entry point for the RocketMQ Dashboard application
fn main() -> anyhow::Result<()> {
    let environment_filter = rocketmq_observability::read_rust_log()?;
    let resolved_filter =
        rocketmq_observability::LogFilterResolver::resolve(rocketmq_observability::LogFilterInputs {
            environment: environment_filter.as_deref(),
            ..rocketmq_observability::LogFilterInputs::default()
        })?;
    let mut bootstrap = rocketmq_observability::TelemetryBootstrapConfig::default();
    bootstrap.observability.service_name = "rocketmq-dashboard-gpui".to_string();
    bootstrap.observability.service_namespace = "rocketmq".to_string();
    bootstrap.observability.node_type = "dashboard".to_string();
    bootstrap.observability.node_id = "gpui".to_string();
    bootstrap.observability.subscriber_install_policy = rocketmq_observability::SubscriberInstallPolicy::Required;
    let telemetry_guard = rocketmq_observability::install_global_with_filter(&bootstrap, resolved_filter.clone())?;
    info!(
        service = "rocketmq-dashboard-gpui",
        effective_filter = resolved_filter.filter(),
        filter_source = %resolved_filter.source(),
        subscriber_installed = telemetry_guard.subscriber_install_status().installed,
        reload_enabled = bootstrap.logging.reload.enabled,
        "GPUI telemetry bootstrap initialized"
    );

    info!("Starting RocketMQ Dashboard");

    let runtime = DesktopClientRuntime::new(telemetry_guard.handle())?;
    let auth = DesktopAuthState::from_process_environment();
    let config_store = DesktopConfigStore::from_environment(runtime.component("config-store"))?;
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
            error!(error = %error, "Unable to create the RocketMQ Dashboard window");
        }
    });
    let runtime_shutdown = runtime.shutdown(provider);
    match &runtime_shutdown {
        Ok(report) => info!(shutdown_report = %report.to_json(), "GPUI runtime shutdown completed"),
        Err(error) => error!(error = %error, "GPUI runtime shutdown failed"),
    }
    let telemetry_shutdown = telemetry_guard.shutdown().into_result();
    runtime_shutdown?;
    telemetry_shutdown?;
    Ok(())
}
