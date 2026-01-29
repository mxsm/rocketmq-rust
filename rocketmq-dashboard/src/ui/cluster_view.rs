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

//! Cluster view component
//!
//! This module provides the Cluster management view for monitoring
//! and managing RocketMQ clusters.

use gpui::*;
use gpui_component::scroll::ScrollableElement;
use gpui_component::StyledExt;

/// Cluster view containing cluster information and metrics
pub struct ClusterView {}

impl ClusterView {
    /// Create a new Cluster view instance
    pub fn new() -> Self {
        Self {}
    }

    /// Render the complete Cluster page layout
    fn render_cluster(&self) -> Div {
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
            .flex()
            .flex_col()
            .gap_2()
            .child(
                div()
                    .text_2xl()
                    .font_bold()
                    .text_color(rgb(0x1D1D1F))
                    .line_height(px(48.0))
                    .child("Cluster Management"),
            )
            .child(
                div()
                    .text_base()
                    .text_color(rgb(0x86868B))
                    .line_height(px(28.0))
                    .child("Monitor and manage your RocketMQ clusters"),
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
            .gap_8()
            .min_h_0()
            .child(self.render_cluster_selector())
            .child(self.render_stats_row())
            .child(self.render_broker_instances())
    }

    /// Render the cluster selector dropdown
    fn render_cluster_selector(&self) -> Div {
        div()
            .w_full()
            .flex()
            .items_center()
            .gap_4()
            .child(
                div()
                    .text_sm()
                    .font_semibold()
                    .text_color(rgb(0x1D1D1F))
                    .child("Cluster"),
            )
            .child(
                div()
                    .flex_1()
                    .max_w(px(400.0))
                    .h(px(56.0))
                    .flex()
                    .items_center()
                    .justify_between()
                    .px_4()
                    .bg(white())
                    .rounded(px(14.0))
                    .border_2()
                    .border_color(rgb(0x007AFF))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_3()
                            .flex_1()
                            .child(
                                div()
                                    .text_base()
                                    .font_semibold()
                                    .text_color(rgb(0x1D1D1F))
                                    .child("DefaultCluster"),
                            )
                            .child(
                                div()
                                    .w(px(40.0))
                                    .h(px(40.0))
                                    .rounded(px(10.0))
                                    .bg(rgb(0xE3F2FD))
                                    .flex()
                                    .items_center()
                                    .justify_center()
                                    .child(div().text_2xl().text_color(rgb(0x007AFF)).child("⛁")),
                            )
                            .child(
                                div().flex().flex_col().gap(px(2.0)).child(
                                    div()
                                        .text_xs()
                                        .text_color(rgb(0x86868B))
                                        .child("3 brokers • 192 topics"),
                                ),
                            ),
                    )
                    .child(div().text_3xl().text_color(rgb(0x86868B)).child("▼")),
            )
    }

    /// Render the statistics row
    fn render_stats_row(&self) -> Div {
        div()
            .flex()
            .flex_row()
            .gap_5()
            .child(self.render_stat_card("Total Brokers".to_string(), "3".to_string()))
            .child(self.render_stat_card("Active Brokers".to_string(), "3".to_string()))
            .child(self.render_stat_card("Produce TPS".to_string(), "2,400.8".to_string()))
            .child(self.render_stat_card("Consume TPS".to_string(), "1,870.6".to_string()))
    }

    /// Render a single statistics card
    fn render_stat_card(&self, title: String, value: String) -> Div {
        div()
            .flex_1()
            .min_w(px(200.0))
            .h(px(100.0))
            .bg(white())
            .rounded(px(16.0))
            .p_6()
            .flex()
            .flex_col()
            .justify_between()
            .gap_3()
            .border_1()
            .border_color(rgb(0xE5E5E7))
            .child(div().text_xs().font_medium().text_color(rgb(0x86868B)).child(title))
            .child(
                div()
                    .text_2xl()
                    .font_bold()
                    .text_color(rgb(0x1D1D1F))
                    .line_height(px(40.0))
                    .child(value),
            )
    }

    /// Render the broker instances section
    fn render_broker_instances(&self) -> impl IntoElement {
        // Broker cards container - this will be wrapped by overflow_y_scrollbar()
        let broker_cards = div()
            .flex()
            .flex_col()
            .gap_6()
            .pb_6()
            .child(self.render_broker_card(
                "mxsm",
                "192.168.192.1:10911",
                true,
                "1,024",
                "850 GB",
                "2.4M/h",
                "1.8M/h",
                rgb(0x007AFF),
                rgb(0xE3F2FD),
            ))
            .child(self.render_broker_card(
                "broker-a",
                "192.168.1.101:10911",
                true,
                "1,024",
                "920 GB",
                "1.2M/h",
                "980K/h",
                rgb(0xFF9500),
                rgb(0xFFF3E0),
            ))
            .child(self.render_broker_card(
                "broker-b",
                "192.168.1.102:10911",
                true,
                "1,024",
                "780 GB",
                "1.5M/h",
                "1.2M/h",
                rgb(0x34C759),
                rgb(0xE8F5E9),
            ))
            .child(self.render_broker_card(
                "broker-c",
                "192.168.1.103:10911",
                true,
                "1,024",
                "650 GB",
                "900K/h",
                "750K/h",
                rgb(0x5856D6),
                rgb(0xEDE7F6),
            ))
            .child(self.render_broker_card(
                "broker-d",
                "192.168.1.104:10911",
                true,
                "1,024",
                "720 GB",
                "1.1M/h",
                "890K/h",
                rgb(0xFF2D55),
                rgb(0xFFEBEE),
            ))
            .child(self.render_broker_card(
                "broker-e",
                "192.168.1.105:10911",
                false,
                "1,024",
                "500 GB",
                "0/h",
                "0/h",
                rgb(0x8E8E93),
                rgb(0xF2F2F7),
            ))
            .child(self.render_broker_card(
                "broker-f",
                "192.168.1.106:10911",
                false,
                "1,024",
                "500 GB",
                "0/h",
                "0/h",
                rgb(0x8E8E93),
                rgb(0xF2F2F7),
            ));

        // Main section container
        div()
            .id("broker-section")
            .flex_1()
            .flex()
            .flex_col()
            .gap_6()
            .min_h_0()
            .child(
                div()
                    .text_2xl()
                    .font_bold()
                    .text_color(rgb(0x1D1D1F))
                    .line_height(px(32.0))
                    .child("Broker Instances"),
            )
            .child(
                // Scrollable container - use overflow_y_scrollbar() which returns Scrollable
                div()
                    .id("broker-scroll-area")
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scrollbar()
                    .child(broker_cards),
            )
    }

    /// Render a single broker card
    fn render_broker_card(
        &self,
        name: &str,
        address: &str,
        is_running: bool,
        partitions: &str,
        disk_usage: &str,
        produce_tps: &str,
        consume_tps: &str,
        icon_color: Rgba,
        icon_bg: Rgba,
    ) -> Div {
        let name = name.to_string();
        let address = address.to_string();
        let partitions = partitions.to_string();
        let disk_usage = disk_usage.to_string();
        let produce_tps = produce_tps.to_string();
        let consume_tps = consume_tps.to_string();

        div()
            .w_full()
            .bg(white())
            .rounded(px(24.0))
            .p_8()
            .flex()
            .flex_col()
            .gap_6()
            .border_1()
            .border_color(rgb(0xE5E5E7))
            .child(self.render_broker_card_header(name, address, is_running, icon_color, icon_bg))
            .child(self.render_broker_card_content(partitions, disk_usage, produce_tps, consume_tps))
            .child(self.render_broker_card_footer())
    }

    /// Render broker card header
    fn render_broker_card_header(
        &self,
        name: String,
        address: String,
        _is_running: bool,
        icon_color: Rgba,
        icon_bg: Rgba,
    ) -> Div {
        div()
            .w_full()
            .flex()
            .items_center()
            .gap_4()
            .child(
                div()
                    .w(px(52.0))
                    .h(px(52.0))
                    .rounded(px(12.0))
                    .bg(icon_bg)
                    .flex()
                    .items_center()
                    .justify_center()
                    .child(div().text_3xl().text_color(icon_color).child("💾")),
            )
            .child(
                div()
                    .flex_1()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .child(div().text_lg().font_bold().text_color(rgb(0x1D1D1F)).child(name))
                    .child(div().text_sm().text_color(rgb(0x007AFF)).child(address)),
            )
            .child(
                div()
                    .px(px(14.0))
                    .py_1()
                    .rounded(px(20.0))
                    .bg(rgb(0xE8F5E9))
                    .flex()
                    .items_center()
                    .gap(px(6.0))
                    .child(div().w(px(6.0)).h(px(6.0)).rounded(px(4.0)).bg(rgb(0x34C759)))
                    .child(
                        div()
                            .text_xs()
                            .font_semibold()
                            .text_color(rgb(0x34C759))
                            .child("Running"),
                    ),
            )
    }

    /// Render broker card content
    fn render_broker_card_content(
        &self,
        partitions: String,
        disk_usage: String,
        produce_tps: String,
        consume_tps: String,
    ) -> Div {
        div()
            .w_full()
            .flex()
            .flex_col()
            .gap_6()
            .child(
                div()
                    .w_full()
                    .flex()
                    .gap_12()
                    .child(self.render_broker_info_item("Partitions", &partitions))
                    .child(self.render_broker_info_item("Disk Usage", &disk_usage)),
            )
            .child(
                div()
                    .w_full()
                    .flex()
                    .gap_12()
                    .child(self.render_broker_info_item("Produce TPS", &produce_tps))
                    .child(self.render_broker_info_item("Consume TPS", &consume_tps)),
            )
    }

    /// Render a single broker info item
    fn render_broker_info_item(&self, label: &str, value: &str) -> Div {
        let label = label.to_string();
        let value = value.to_string();
        div()
            .flex_1()
            .flex()
            .flex_col()
            .gap_4()
            .child(div().text_xs().text_color(rgb(0x86868B)).child(label))
            .child(div().text_base().font_semibold().text_color(rgb(0x1D1D1F)).child(value))
    }

    /// Render broker card footer with action buttons
    fn render_broker_card_footer(&self) -> Div {
        div()
            .w_full()
            .flex()
            .gap_3()
            .child(
                div()
                    .flex_1()
                    .h(px(44.0))
                    .flex()
                    .items_center()
                    .justify_center()
                    .rounded(px(8.0))
                    .bg(rgb(0x007AFF))
                    .cursor_pointer()
                    .child(div().text_sm().font_semibold().text_color(white()).child("Status")),
            )
            .child(
                div()
                    .flex_1()
                    .h(px(44.0))
                    .flex()
                    .items_center()
                    .justify_center()
                    .rounded(px(8.0))
                    .bg(rgb(0xF5F5F7))
                    .cursor_pointer()
                    .child(
                        div()
                            .text_sm()
                            .font_semibold()
                            .text_color(rgb(0x1D1D1F))
                            .child("Config"),
                    ),
            )
    }
}

impl Render for ClusterView {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        self.render_cluster()
    }
}

impl IntoElement for ClusterView {
    type Element = Div;

    fn into_element(self) -> Self::Element {
        self.render_cluster()
    }
}
