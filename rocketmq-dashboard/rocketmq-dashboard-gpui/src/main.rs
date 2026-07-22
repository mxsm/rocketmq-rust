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

//! RocketMQ Dashboard - Modern GUI for Apache RocketMQ monitoring
//!
//! This application provides a real-time dashboard for monitoring
//! and managing RocketMQ clusters, topics, and message flows.

mod ui;

use gpui::*;
use gpui_component::Root;
use tracing::info;
use ui::dashboard_view::DashboardView;

/// Main dashboard application struct
pub struct RocketmqDashboard {
    dashboard_view: Entity<DashboardView>,
}

impl RocketmqDashboard {
    fn new(cx: &mut Context<Self>) -> Self {
        Self {
            dashboard_view: cx.new(|_| DashboardView::new()),
        }
    }
}

impl Render for RocketmqDashboard {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .flex()
            .bg(rgb(0xF5F5F7))
            .child(self.dashboard_view.clone())
    }
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

    let app = Application::new();

    app.run(move |cx| {
        // This must be called before using any GPUI Component features.
        gpui_component::init(cx);

        cx.spawn(async move |cx| {
            cx.open_window(
                WindowOptions {
                    window_bounds: Some(WindowBounds::Windowed(Bounds {
                        origin: Point {
                            x: px(100.0),
                            y: px(100.0),
                        },
                        size: gpui::Size {
                            width: px(1440.0),
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
                    let view = cx.new(RocketmqDashboard::new);
                    // This first level on the window, should be a Root.
                    cx.new(|cx| Root::new(view, window, cx))
                },
            )
        })
        .detach();
    });
    telemetry_guard.shutdown().into_result()?;
    Ok(())
}
