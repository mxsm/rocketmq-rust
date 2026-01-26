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
pub struct RocketmqDashboard;

impl Render for RocketmqDashboard {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div().size_full().flex().bg(rgb(0xF5F5F7)).child(DashboardView::new())
    }
}

/// Main entry point for the RocketMQ Dashboard application
fn main() {
    // Initialize logging
    tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).init();

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
                    let view = cx.new(|_| RocketmqDashboard);
                    // This first level on the window, should be a Root.
                    cx.new(|cx| Root::new(view, window, cx))
                },
            )
        })
        .detach();
    });
}
