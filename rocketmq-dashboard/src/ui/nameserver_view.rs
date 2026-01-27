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

//! NameServer view component
//!
//! This module provides the NameServer management view for monitoring
//! and managing RocketMQ NameServer instances.

use gpui::*;
use gpui_component::StyledExt;

/// NameServer view containing server information and metrics
pub struct NameserverView {}

impl NameserverView {
    /// Create a new NameServer view instance
    pub fn new() -> Self {
        Self {}
    }

    /// Render the complete NameServer page layout
    fn render_nameserver(&self) -> Div {
        div()
            .size_full()
            .flex()
            .flex_col()
            .bg(rgb(0xF5F5F7))
            .child(self.render_header())
            .child(self.render_content())
    }

    /// Render the page header
    fn render_header(&self) -> Div {
        div()
            .w_full()
            .h(px(60.0))
            .flex()
            .items_center()
            .justify_between()
            .px_6()
            .border_b_1()
            .border_color(rgb(0xE5E5E7))
            .bg(white())
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .child(
                        div()
                            .text_xl()
                            .font_semibold()
                            .text_color(rgb(0x1D1D1F))
                            .child("NameServer"),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(rgb(0x86868B))
                            .child("Manage and monitor NameServer instances"),
                    ),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .px_4()
                    .py_2()
                    .bg(rgb(0x007AFF))
                    .rounded(px(6.0))
                    .cursor_pointer()
                    .child(
                        div()
                            .text_sm()
                            .font_semibold()
                            .text_color(rgb(0xFFFFFF))
                            .child("+ Add Node"),
                    ),
            )
    }

    /// Render the main content area
    fn render_content(&self) -> Div {
        div()
            .flex_1()
            .flex()
            .flex_col()
            .p_6()
            .bg(rgb(0xF5F5F7))
            .gap_6()
            .child(self.render_stats_row())
            .child(self.render_nameserver_list())
    }

    /// Render the statistics row
    fn render_stats_row(&self) -> Div {
        div()
            .flex()
            .flex_row()
            .gap_4()
            .child(self.render_stat_card("Total Nameservers".to_string(), "3".to_string(), rgb(0x007AFF)))
            .child(self.render_stat_card("Active Brokers".to_string(), "12".to_string(), rgb(0x34C759)))
            .child(self.render_stat_card("Total Topics".to_string(), "48".to_string(), rgb(0xFF9500)))
            .child(self.render_stat_card("Total Groups".to_string(), "36".to_string(), rgb(0x5856D6)))
    }

    /// Render a single statistics card
    fn render_stat_card(&self, title: String, value: String, color: Rgba) -> Div {
        div()
            .flex_1()
            .min_w(px(200.0))
            .h(px(120.0))
            .bg(white())
            .rounded(px(12.0))
            .p_5()
            .flex()
            .flex_col()
            .justify_between()
            .border_1()
            .border_color(rgb(0xE5E5E7))
            .child(div().text_sm().text_color(rgb(0x86868B)).child(title))
            .child(div().text_3xl().font_semibold().text_color(color).child(value))
    }

    /// Render the NameServer list table
    fn render_nameserver_list(&self) -> Div {
        div()
            .flex_1()
            .bg(white())
            .rounded(px(12.0))
            .border_1()
            .border_color(rgb(0xE5E5E7))
            .flex()
            .flex_col()
            .child(self.render_table_header())
            .child(self.render_table_rows())
    }

    /// Render the table header
    fn render_table_header(&self) -> Div {
        div()
            .w_full()
            .h(px(48.0))
            .flex()
            .items_center()
            .px_6()
            .border_b_1()
            .border_color(rgb(0xE5E5E7))
            .bg(rgb(0xF5F5F7))
            .child(
                div()
                    .flex()
                    .flex_row()
                    .w_full()
                    .gap_6()
                    .child(self.render_table_header_cell("Address"))
                    .child(self.render_table_header_cell("Port"))
                    .child(self.render_table_header_cell("Status"))
                    .child(self.render_table_header_cell("Brokers"))
                    .child(self.render_table_header_cell("Uptime")),
            )
    }

    /// Render a table header cell
    fn render_table_header_cell(&self, text: &str) -> Div {
        let text = text.to_string();
        div()
            .flex_1()
            .text_sm()
            .font_semibold()
            .text_color(rgb(0x1D1D1F))
            .child(text)
    }

    /// Render the table rows
    fn render_table_rows(&self) -> Div {
        div()
            .flex_1()
            .flex()
            .flex_col()
            .child(self.render_table_row("192.168.1.10", "9876", "Running", "4", "5d 12h 30m", rgb(0x34C759)))
            .child(self.render_table_row("192.168.1.11", "9876", "Running", "4", "5d 12h 28m", rgb(0x34C759)))
            .child(self.render_table_row("192.168.1.12", "9876", "Running", "4", "5d 12h 25m", rgb(0x34C759)))
    }

    /// Render a single table row
    fn render_table_row(
        &self,
        address: &str,
        port: &str,
        status: &str,
        brokers: &str,
        uptime: &str,
        status_color: Rgba,
    ) -> Div {
        div()
            .w_full()
            .h(px(56.0))
            .flex()
            .items_center()
            .px_6()
            .border_b_1()
            .border_color(rgb(0xE5E5E7))
            .child(
                div()
                    .flex()
                    .flex_row()
                    .w_full()
                    .gap_6()
                    .child(self.render_table_cell(address))
                    .child(self.render_table_cell(port))
                    .child(self.render_status_cell(status, status_color))
                    .child(self.render_table_cell(brokers))
                    .child(self.render_table_cell(uptime)),
            )
    }

    /// Render a table cell
    fn render_table_cell(&self, text: &str) -> Div {
        let text = text.to_string();
        div().flex_1().text_sm().text_color(rgb(0x1D1D1F)).child(text)
    }

    /// Render a status cell with colored indicator
    fn render_status_cell(&self, text: &str, color: Rgba) -> Div {
        let text = text.to_string();
        div()
            .flex_1()
            .flex()
            .items_center()
            .gap_2()
            .child(div().w(px(8.0)).h(px(8.0)).rounded_full().bg(color))
            .child(div().text_sm().text_color(color).child(text))
    }
}

impl Render for NameserverView {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        self.render_nameserver()
    }
}
