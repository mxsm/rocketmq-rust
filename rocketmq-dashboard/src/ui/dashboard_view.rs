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

//! Dashboard view component
//!
//! This module provides the main dashboard view with sidebar navigation
//! and content area for displaying RocketMQ metrics.
//!
//! # Icons
//!
//! ## Current Implementation
//!
//! The dashboard uses emoji icons for all menu items:
//! - 📊 Dashboard
//! - 🌐 NameServer
//! - ☁️ Cluster
//! - 📚 Topic
//! - 👥 Consumer
//! - 📤 Producer
//! - ✉️ Message
//! - ⚠️ DLQ Message
//! - 📈 Message Trace
//! - 🔒 ACL Management
//! - ⚙️ Settings
//!
//! ## SVG Icons (Available for Future Use)
//!
//! SVG icon files are provided in `assets/icons/` directory for all menu items.
//! These can be used to implement custom icon rendering if needed:
//! - `dashboard.svg`, `nameserver.svg`, `cluster.svg`, `topic.svg`
//! - `consumer.svg`, `producer.svg`, `message.svg`, `dlq.svg`
//! - `message_trace.svg`, `acl.svg`, `settings.svg`, `calendar.svg`
//!
//! To use SVG icons instead of emoji, add an SVG rendering library
//! (such as `resvg`) and update the `MenuItem` rendering to use the `svg` field.
//!
//! Example migration path:
//! 1. Add `resvg` dependency to Cargo.toml
//! 2. Create an `svg_icon()` helper function that renders SVG to image
//! 3. Replace `.child(item.icon)` with `.child(svg_icon(item.svg))`

use gpui::prelude::FluentBuilder;
use gpui::*;

/// Icon data mapping menu items to their display information
struct MenuItem {
    /// Display name
    name: &'static str,
    /// Icon emoji
    icon: &'static str,
}

/// Dashboard view containing sidebar and main content area
pub struct DashboardView {
    /// The root div element containing all dashboard UI
    div: Div,
}

impl DashboardView {
    /// Create a new dashboard view instance
    pub fn new() -> Self {
        Self {
            div: Self::render_dashboard(),
        }
    }

    /// Render the complete dashboard layout with sidebar and content
    fn render_dashboard() -> Div {
        div()
            .size_full()
            .flex()
            .flex_row()
            .child(Self::render_sidebar())
            .child(Self::render_main_content())
    }

    /// Render the sidebar navigation with RocketMQ logo and menu items
    fn render_sidebar() -> Div {
        let menu_items: [MenuItem; 11] = [
            MenuItem {
                name: "Dashboard",
                icon: "📊",
            },
            MenuItem {
                name: "NameServer",
                icon: "🌐",
            },
            MenuItem {
                name: "Cluster",
                icon: "☁️",
            },
            MenuItem {
                name: "Topic",
                icon: "📚",
            },
            MenuItem {
                name: "Consumer",
                icon: "👥",
            },
            MenuItem {
                name: "Producer",
                icon: "📤",
            },
            MenuItem {
                name: "Message",
                icon: "✉️",
            },
            MenuItem {
                name: "DLQMessage",
                icon: "⚠️",
            },
            MenuItem {
                name: "MessageTrace",
                icon: "📈",
            },
            MenuItem {
                name: "ACL Management",
                icon: "🔒",
            },
            MenuItem {
                name: "Settings",
                icon: "⚙️",
            },
        ];

        div()
            .w(px(280.0)) // Fixed width for sidebar (standard design pattern)
            .h_full() // Full height - responsive to window
            .flex()
            .flex_col()
            .bg(rgb(0xFFFFFF))
            .border_r_1()
            .border_color(rgb(0xE5E5E7))
            .p_6()
            .gap_2()
            .child(Self::render_logo())
            .children(menu_items.iter().map(|item| {
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .h(px(44.0))
                    .px_4()
                    .cursor_pointer()
                    .rounded(px(8.0))
                    .when(item.name == "Dashboard", |div| div.bg(rgb(0xF5F5F7)))
                    .child(
                        div()
                            .text_base()
                            .text_color(if item.name == "Dashboard" {
                                rgb(0x007AFF)
                            } else {
                                rgb(0x86868B)
                            })
                            .child(item.icon),
                    )
                    .child(
                        div()
                            .text_base()
                            .font_weight(if item.name == "Dashboard" {
                                FontWeight::SEMIBOLD
                            } else {
                                FontWeight::NORMAL
                            })
                            .text_color(rgb(0x1D1D1F))
                            .child(item.name),
                    )
            }))
    }

    /// Render the logo section
    fn render_logo() -> Div {
        div()
            .flex()
            .flex_col()
            .gap_2()
            .mb_4()
            .child(
                div()
                    .text_2xl()
                    .font_weight(FontWeight::BOLD)
                    .text_color(rgb(0x1D1D1F))
                    .child("RocketMQ-Rust"),
            )
            .child(div().text_sm().text_color(rgb(0x86868B)).child("Operations Dashboard"))
    }

    /// Render the main content area
    fn render_main_content() -> Div {
        div()
            .flex_1()
            .h_full()
            .flex()
            .flex_col()
            .p(px(40.0))
            .gap(px(24.0))
            .child(Self::render_page_header())
            .child(Self::render_content_columns())
    }

    /// Render the page header
    fn render_page_header() -> Div {
        div().flex().items_center().gap_3().child(
            div()
                .text_2xl()
                .font_weight(FontWeight::BOLD)
                .text_color(rgb(0x1D1D1F))
                .child("Dashboard"),
        )
    }

    /// Render the two-column content layout
    fn render_content_columns() -> Div {
        div()
            .flex()
            .flex_row()
            .gap_5()
            .flex_1() // Take available space
            .child(Self::render_left_column())
            .child(Self::render_right_column())
    }

    /// Render the left column with overview and charts
    fn render_left_column() -> Div {
        div()
            .flex_1() // Responsive: takes 50% of available space
            .min_w(px(300.0)) // Minimum width to prevent squishing
            .flex()
            .flex_col()
            .gap_5()
            .child(Self::render_broker_overview_card())
            .child(Self::render_broker_top10_card())
            .child(Self::render_topic_top10_card())
    }

    /// Render the right column with date picker and trends
    fn render_right_column() -> Div {
        div()
            .flex_1() // Responsive: takes 50% of available space
            .min_w(px(300.0)) // Minimum width to prevent squishing
            .flex()
            .flex_col()
            .gap_5()
            .child(Self::render_date_picker_card())
            .child(Self::render_broker_trend_card())
            .child(Self::render_topic_trend_card())
    }

    /// Render Broker Overview card
    fn render_broker_overview_card() -> Div {
        let info = [
            ("Broker name:", "mxsm"),
            ("Broker address:", "192.168.192.1:10911"),
            ("Total messages received today:", "384"),
            ("Today Produce Count:", "0"),
            ("Yesterday Produce Count:", "0"),
        ];

        div()
            .w_full()
            .rounded(px(12.0))
            .bg(rgb(0xFFFFFF))
            .border_1()
            .border_color(rgb(0xE5E5E7))
            .p_5()
            .flex()
            .flex_col()
            .gap_4()
            .child(
                div()
                    .text_base()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(rgb(0x1D1D1F))
                    .child("Broker Overview"),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap_3()
                    .children(info.iter().map(|(label, value)| {
                        div()
                            .flex()
                            .flex_row()
                            .items_center()
                            .gap_3()
                            .child(div().w(px(180.0)).text_sm().text_color(rgb(0x86868B)).child(*label))
                            .child(
                                div()
                                    .flex_1()
                                    .text_sm()
                                    .font_weight(FontWeight::MEDIUM)
                                    .text_color(rgb(0x1D1D1F))
                                    .child(*value),
                            )
                    })),
            )
    }

    /// Render Broker TOP 10 card
    fn render_broker_top10_card() -> Div {
        div()
            .w_full()
            .rounded(px(12.0))
            .bg(rgb(0xFFFFFF))
            .border_1()
            .border_color(rgb(0xE5E5E7))
            .p_5()
            .flex()
            .flex_col()
            .gap_4()
            .child(
                div()
                    .text_base()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(rgb(0x1D1D1F))
                    .child("Broker TOP 10"),
            )
            .child(
                div()
                    .w_full()
                    .h(px(180.0))
                    .rounded(px(8.0))
                    .bg(rgb(0xFAFAFA))
                    .border_1()
                    .border_color(rgb(0xE5E5E7))
                    .p_4()
                    .flex()
                    .flex_col()
                    .gap_3()
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .child(div().w(px(8.0)).h(px(8.0)).rounded(px(4.0)).bg(rgb(0x007AFF)))
                            .child(
                                div()
                                    .text_xs()
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .text_color(rgb(0x1D1D1F))
                                    .child("TotalMsg: 400+"),
                            ),
                    )
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_3()
                            .child(
                                div()
                                    .w(px(80.0))
                                    .text_sm()
                                    .font_weight(FontWeight::MEDIUM)
                                    .text_color(rgb(0x1D1D1F))
                                    .child("mxsm"),
                            )
                            .child(
                                div()
                                    .flex_1()
                                    .h(px(32.0))
                                    .bg(rgb(0xF5F5F7))
                                    .rounded(px(6.0))
                                    .relative()
                                    .child(div().h_full().w(px(450.0)).bg(rgb(0x007AFF)).rounded(px(6.0))),
                            )
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::BOLD)
                                    .text_color(rgb(0x007AFF))
                                    .child("400"),
                            ),
                    ),
            )
    }

    /// Render Topic TOP 10 card
    fn render_topic_top10_card() -> Div {
        div()
            .w_full()
            .rounded(px(12.0))
            .bg(rgb(0xFFFFFF))
            .border_1()
            .border_color(rgb(0xE5E5E7))
            .p_5()
            .flex()
            .flex_col()
            .gap_4()
            .child(
                div()
                    .text_base()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(rgb(0x1D1D1F))
                    .child("Topic TOP 10"),
            )
            .child(
                div()
                    .w_full()
                    .h(px(180.0))
                    .rounded(px(8.0))
                    .bg(rgb(0xFAFAFA))
                    .border_1()
                    .border_color(rgb(0xE5E5E7))
                    .p_4()
                    .flex()
                    .flex_col()
                    .gap_3()
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .child(div().w(px(8.0)).h(px(8.0)).rounded(px(4.0)).bg(rgb(0x34C759)))
                            .child(
                                div()
                                    .text_xs()
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .text_color(rgb(0x1D1D1F))
                                    .child("Total: 456"),
                            ),
                    )
                    .children(
                        [
                            ("%RETRY%mxsm-a", px(280.0), rgb(0x34C759), "240"),
                            ("TopicTest", px(170.0), rgb(0xFF9500), "144"),
                            ("HALF_TOPIC", px(85.0), rgb(0xFF3B30), "72"),
                        ]
                        .iter()
                        .map(|(name, width, color, value)| {
                            div()
                                .flex()
                                .items_center()
                                .gap_3()
                                .child(
                                    div()
                                        .w(px(140.0))
                                        .text_sm()
                                        .font_weight(FontWeight::MEDIUM)
                                        .text_color(rgb(0x1D1D1F))
                                        .child(*name),
                                )
                                .child(
                                    div()
                                        .flex_1()
                                        .h(px(32.0))
                                        .bg(rgb(0xF5F5F7))
                                        .rounded(px(6.0))
                                        .child(div().h_full().w(*width).bg(*color).rounded(px(6.0))),
                                )
                                .child(
                                    div()
                                        .text_sm()
                                        .font_weight(FontWeight::BOLD)
                                        .text_color(*color)
                                        .child(*value),
                                )
                        }),
                    ),
            )
    }

    /// Render Date Picker card
    fn render_date_picker_card() -> Div {
        div()
            .w_full()
            .rounded(px(12.0))
            .bg(rgb(0xFFFFFF))
            .border_1()
            .border_color(rgb(0xE5E5E7))
            .p_5()
            .flex()
            .flex_col()
            .gap_4()
            .child(
                div()
                    .text_base()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(rgb(0x1D1D1F))
                    .child("Date Picker"),
            )
            .child(
                div()
                    .w_full()
                    .h(px(48.0))
                    .rounded(px(8.0))
                    .bg(rgb(0xF5F5F7))
                    .border_1()
                    .border_color(rgb(0xE5E5E7))
                    .px_4()
                    .flex()
                    .items_center()
                    .justify_between()
                    .child(div().text_base().text_color(rgb(0x1D1D1F)).child("2026-01-26"))
                    .child(
                        div()
                            .text_xl()
                            .font_family("Symbol")
                            .text_color(rgb(0x86868B))
                            .child("\u{F1E5}"), // calendar_today
                    ),
            )
    }

    /// Render Broker 5min trend card
    fn render_broker_trend_card() -> Div {
        div()
            .w_full()
            .rounded(px(12.0))
            .bg(rgb(0xFFFFFF))
            .border_1()
            .border_color(rgb(0xE5E5E7))
            .p_5()
            .flex()
            .flex_col()
            .gap_4()
            .child(
                div()
                    .text_base()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(rgb(0x1D1D1F))
                    .child("Broker 5min trend"),
            )
            .child(Self::render_trend_chart("mxsm0".to_string(), rgb(0xFF3B30)))
    }

    /// Render Topic 5min trend card
    fn render_topic_trend_card() -> Div {
        div()
            .w_full()
            .rounded(px(12.0))
            .bg(rgb(0xFFFFFF))
            .border_1()
            .border_color(rgb(0xE5E5E7))
            .p_5()
            .flex()
            .flex_col()
            .gap_4()
            .child(
                div()
                    .text_base()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(rgb(0x1D1D1F))
                    .child("Topic 5min trend"),
            )
            .child(Self::render_trend_chart("broker-mxsm-a".to_string(), rgb(0x007AFF)))
    }

    /// Render a trend chart
    fn render_trend_chart(series_name: String, color: Rgba) -> Div {
        div()
            .w_full()
            .h(px(240.0))
            .rounded(px(8.0))
            .bg(rgb(0xFAFAFA))
            .border_1()
            .border_color(rgb(0xE5E5E7))
            .p_5()
            .flex()
            .flex_col()
            .gap_3()
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .child(div().w(px(12.0)).h(px(12.0)).rounded(px(6.0)).bg(color))
                    .child(
                        div()
                            .text_xs()
                            .font_weight(FontWeight::MEDIUM)
                            .text_color(rgb(0x1D1D1F))
                            .child(series_name),
                    ),
            )
            .child(
                div()
                    .flex_1()
                    .h(px(160.0))
                    .flex()
                    .flex_row()
                    .items_end()
                    .gap_6()
                    .children((0..6).map(|_| div().w(px(12.0)).h(px(12.0)).rounded(px(6.0)).bg(color)))
                    .px_2(),
            )
            .child(
                div()
                    .flex()
                    .flex_row()
                    .justify_between()
                    .w_full()
                    .child(div().text_xs().text_color(rgb(0x86868B)).child("19:41:00"))
                    .child(div().text_xs().text_color(rgb(0x86868B)).child("20:23:00")),
            )
    }
}

impl IntoElement for DashboardView {
    type Element = Div;

    fn into_element(self) -> Self::Element {
        self.div
    }
}

impl Default for DashboardView {
    fn default() -> Self {
        Self::new()
    }
}
