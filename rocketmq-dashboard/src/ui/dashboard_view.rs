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

use gpui::prelude::FluentBuilder;
use gpui::*;

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
            .child(
                div()
                    .w(px(260.0))
                    .h_full()
                    .flex()
                    .flex_col()
                    .bg(rgb(0xFAFAFA))
                    .border_r_1()
                    .border_color(rgb(0xE5E5E7))
                    .child(Self::render_sidebar()),
            )
            .child(
                div()
                    .flex_1()
                    .h_full()
                    .p_8()
                    .flex()
                    .flex_col()
                    .gap_6()
                    .child(
                        div()
                            .text_xl()
                            .font_weight(FontWeight::BOLD)
                            .text_color(rgb(0x1D1D1F))
                            .child("Dashboard"),
                    )
                    .child(
                        div()
                            .text_base()
                            .text_color(rgb(0x86868B))
                            .child("Overview of your RocketMQ instance"),
                    ),
            )
    }

    /// Render the sidebar navigation with RocketMQ logo and menu items
    fn render_sidebar() -> Div {
        div()
            .flex()
            .flex_col()
            .gap_6()
            .p_4()
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .child(div().w(px(32.0)).h(px(32.0)).bg(rgb(0x0071E3)).rounded(px(8.0)))
                    .child(
                        div()
                            .text_lg()
                            .font_weight(FontWeight::BOLD)
                            .text_color(rgb(0x1D1D1F))
                            .child("RocketMQ"),
                    ),
            )
            .children(
                ["Dashboard", "Message", "Topic", "Consumer", "Cluster", "Broker"]
                    .iter()
                    .enumerate()
                    .map(|(i, item)| {
                        div()
                            .flex()
                            .items_center()
                            .gap_3()
                            .h(px(40.0))
                            .px_3()
                            .cursor_pointer()
                            .rounded(px(12.0))
                            .when(i == 0, |div: Div| div.bg(rgb(0x0071E3)))
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::MEDIUM)
                                    .text_color(if i == 0 { rgb(0xFFFFFF) } else { rgb(0x86868B) })
                                    .child(*item),
                            )
                    }),
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
