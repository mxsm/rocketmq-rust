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

//! Consumer Subscription Group Management view component
//!
//! This module provides the Consumer Group Management page with filtering,
//! consumer group cards grid, and operations.

use gpui::prelude::FluentBuilder;
use gpui::*;

/// Consumer group type classification
#[derive(Clone, Copy, Debug, PartialEq)]
enum ConsumerType {
    Normal,
    Fifo,
    System,
}

impl ConsumerType {
    fn display_name(&self) -> &'static str {
        match self {
            ConsumerType::Normal => "NORMAL",
            ConsumerType::Fifo => "FIFO",
            ConsumerType::System => "SYSTEM",
        }
    }

    fn color(&self) -> Rgba {
        match self {
            ConsumerType::Normal => rgb(0x007AFF),
            ConsumerType::Fifo => rgb(0x34C759),
            ConsumerType::System => rgb(0xFF9500),
        }
    }

    fn bg_color(&self) -> Rgba {
        match self {
            ConsumerType::Normal => rgb(0xE3F2FD),
            ConsumerType::Fifo => rgb(0xE8F5E9),
            ConsumerType::System => rgb(0xFFF3E0),
        }
    }

    fn icon(&self) -> &'static str {
        match self {
            ConsumerType::Normal => "\u{1F465}",
            ConsumerType::Fifo => "\u{1F5D0}",
            ConsumerType::System => "\u{2699}",
        }
    }
}

/// Consumer group data model
struct ConsumerGroupData {
    name: String,
    group_type: ConsumerType,
    tps: String,
    delay: String,
    clients: u32,
}

impl ConsumerGroupData {
    fn new(name: &str, group_type: ConsumerType, tps: &str, delay: &str, clients: u32) -> Self {
        Self {
            name: name.to_string(),
            group_type,
            tps: tps.to_string(),
            delay: delay.to_string(),
            clients,
        }
    }
}

/// Consumer Management view
pub struct ConsumerView {
    /// Active filter types
    active_filters: Vec<ConsumerType>,
    /// Consumer group list data
    consumer_groups: Vec<ConsumerGroupData>,
    /// Index of consumer whose modal should be shown
    modal_consumer_index: Option<usize>,
}

impl ConsumerView {
    /// Create a new Consumer Management view
    pub fn new() -> Self {
        // Initialize with all filters active
        let active_filters = vec![ConsumerType::Normal, ConsumerType::Fifo, ConsumerType::System];

        // Sample consumer group data
        let consumer_groups = vec![
            ConsumerGroupData::new("please_rename_unique_group_4", ConsumerType::Normal, "2,340", "12ms", 8),
            ConsumerGroupData::new("FIFOConsumerGroup", ConsumerType::Fifo, "1,245", "8ms", 5),
            ConsumerGroupData::new("SYSTEM_HALF_GROUP", ConsumerType::System, "892", "3ms", 2),
            ConsumerGroupData::new("DefaultConsumerGroup", ConsumerType::Normal, "3,456", "15ms", 12),
            ConsumerGroupData::new("OrderConsumerGroup", ConsumerType::Fifo, "987", "5ms", 4),
            ConsumerGroupData::new("SYS_MONITOR_GROUP", ConsumerType::System, "654", "2ms", 3),
        ];

        Self {
            active_filters,
            consumer_groups,
            modal_consumer_index: None,
        }
    }

    /// Render the complete consumer management page
    fn render_page(&self, cx: &mut Context<Self>) -> Div {
        div()
            .size_full()
            .flex()
            .flex_col()
            .relative()
            .child(self.render_main_content(cx))
            .when(self.modal_consumer_index.is_some(), |parent| {
                parent.child(self.render_modal_content_only(cx))
            })
    }

    /// Render the main content area
    fn render_main_content(&self, cx: &mut Context<Self>) -> Div {
        div()
            .flex_1()
            .h_full()
            .flex()
            .flex_col()
            .bg(rgb(0xF5F5F7))
            .p(px(36.0))
            .gap(px(32.0))
            .child(Self::render_page_header())
            .child(self.render_filter_bar(cx))
            .child(self.render_consumer_list(cx))
    }

    /// Render the page header
    fn render_page_header() -> Div {
        div()
            .flex()
            .flex_col()
            .gap_2()
            .child(
                div()
                    .text_3xl()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(rgb(0x1D1D1F))
                    .child("Consumer Subscription Groups"),
            )
            .child(
                div()
                    .text_base()
                    .text_color(rgb(0x98989D))
                    .child("Manage and monitor your RocketMQ consumer groups"),
            )
    }

    /// Render the filter bar
    fn render_filter_bar(&self, cx: &mut Context<Self>) -> Div {
        div()
            .w_full()
            .rounded(px(12.0))
            .bg(rgb(0xFFFFFF))
            .border_1()
            .border_color(rgb(0xE5E5E7))
            .p_5()
            .flex()
            .flex_col()
            .gap_5()
            .child(self.render_filter_header(cx))
            .child(self.render_filter_checkboxes(cx))
    }

    /// Render the filter header with search and actions
    fn render_filter_header(&self, _cx: &mut Context<Self>) -> Div {
        div()
            .flex()
            .items_center()
            .gap_4()
            .w_full()
            .child(
                div()
                    .w(px(320.0))
                    .h(px(40.0))
                    .flex()
                    .items_center()
                    .gap_2()
                    .px_3()
                    .rounded(px(8.0))
                    .bg(rgb(0xF5F5F7))
                    .border_1()
                    .border_color(rgb(0xE5E5E7))
                    .child(div().text_lg().text_color(rgb(0x86868B)).child("\u{1F50D}"))
                    .child(
                        div()
                            .text_sm()
                            .text_color(rgb(0x86868B))
                            .child("Search consumer groups..."),
                    ),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .flex_1()
                    .justify_end()
                    .child(self.render_button("refresh", "Refresh", rgb(0x1D1D1F), false))
                    .child(self.render_button("add", "Add Group", rgb(0xFFFFFF), true)),
            )
    }

    /// Render a button
    fn render_button(&self, icon: &'static str, text: &'static str, text_color: Rgba, is_primary: bool) -> Div {
        let bg = if is_primary { rgb(0x007AFF) } else { rgb(0xF5F5F7) };

        div()
            .flex()
            .items_center()
            .gap_2()
            .px_4()
            .py_2()
            .rounded(px(8.0))
            .bg(bg)
            .border_1()
            .border_color(rgb(0xE5E5E7))
            .cursor_pointer()
            .child(div().text_lg().text_color(text_color).child(icon.to_string()))
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(text_color)
                    .child(text),
            )
    }

    /// Render the filter checkboxes
    fn render_filter_checkboxes(&self, _cx: &mut Context<Self>) -> Div {
        let types = [ConsumerType::Normal, ConsumerType::Fifo, ConsumerType::System];

        div()
            .flex()
            .flex_wrap()
            .gap_4()
            .children(types.iter().map(|consumer_type| {
                let is_active = self.active_filters.contains(consumer_type);
                self.render_filter_checkbox(consumer_type, is_active)
            }))
    }

    /// Render a filter checkbox
    fn render_filter_checkbox(&self, consumer_type: &ConsumerType, is_active: bool) -> impl IntoElement {
        let bg = if is_active { rgb(0x007AFF) } else { rgb(0xFFFFFF) };
        let stroke = if is_active { rgb(0x007AFF) } else { rgb(0xE5E5E7) };

        div()
            .flex()
            .items_center()
            .gap_1()
            .cursor_pointer()
            .child(
                div()
                    .w(px(16.0))
                    .h(px(16.0))
                    .flex()
                    .items_center()
                    .justify_center()
                    .rounded(px(4.0))
                    .bg(bg)
                    .border_1()
                    .border_color(stroke)
                    .when(is_active, |d| {
                        d.child(div().text_xs().text_color(rgb(0xFFFFFF)).child("\u{2713}"))
                    }),
            )
            .child(
                div()
                    .text_sm()
                    .text_color(rgb(0x1D1D1F))
                    .child(consumer_type.display_name()),
            )
    }

    /// Render the consumer group list section
    fn render_consumer_list(&self, cx: &mut Context<Self>) -> Div {
        // Filter consumer groups based on active filters
        let filtered_groups: Vec<_> = self
            .consumer_groups
            .iter()
            .filter(|group| self.active_filters.contains(&group.group_type))
            .collect();

        div()
            .flex()
            .flex_col()
            .gap_6()
            .w_full()
            .flex_1()
            .child(self.render_consumer_list_header(filtered_groups.len()))
            .child(self.render_consumer_grid(filtered_groups, cx))
            .child(self.render_pagination())
    }

    /// Render the consumer list header
    fn render_consumer_list_header(&self, count: usize) -> Div {
        div()
            .flex()
            .items_center()
            .gap_4()
            .w_full()
            .child(
                div()
                    .text_base()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(rgb(0x1D1D1F))
                    .child("Consumer Groups"),
            )
            .child(
                div().px_3().py_1().rounded(px(12.0)).bg(rgb(0xF5F5F7)).child(
                    div()
                        .text_sm()
                        .font_weight(FontWeight::MEDIUM)
                        .text_color(rgb(0x1D1D1F))
                        .child(count.to_string()),
                ),
            )
    }

    /// Render the consumer group cards grid
    fn render_consumer_grid(&self, groups: Vec<&ConsumerGroupData>, cx: &mut Context<Self>) -> Div {
        // Create grid rows with 3 cards each
        let rows: Vec<_> = groups.chunks(3).collect();

        div()
            .flex()
            .flex_col()
            .gap_5()
            .w_full()
            .children(rows.iter().map(|row| {
                div()
                    .flex()
                    .flex_row()
                    .gap_5()
                    .w_full()
                    .children(row.iter().map(|group| self.render_consumer_card(group, cx)))
            }))
    }

    /// Render a single consumer group card
    fn render_consumer_card(&self, group: &ConsumerGroupData, cx: &mut Context<Self>) -> Div {
        let icon_bg = group.group_type.bg_color();

        div()
            .flex_1()
            .min_w(px(280.0))
            .rounded(px(16.0))
            .bg(rgb(0xFFFFFF))
            .border_1()
            .border_color(rgb(0xE5E5E7))
            .p_6()
            .flex()
            .flex_col()
            .gap_4()
            .child(self.render_card_header(group, icon_bg))
            .child(self.render_card_stats(group))
            .child(self.render_card_actions(group, cx))
    }

    /// Render the card header
    fn render_card_header(&self, group: &ConsumerGroupData, icon_bg: Rgba) -> Div {
        div()
            .flex()
            .items_center()
            .justify_between()
            .w_full()
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_4()
                    .child(
                        div()
                            .w(px(56.0))
                            .h(px(56.0))
                            .flex()
                            .items_center()
                            .justify_center()
                            .rounded(px(12.0))
                            .bg(icon_bg)
                            .child(
                                div()
                                    .text_3xl()
                                    .text_color(group.group_type.color())
                                    .child(group.group_type.icon()),
                            ),
                    )
                    .child(
                        div()
                            .text_base()
                            .font_weight(FontWeight::MEDIUM)
                            .text_color(rgb(0x1D1D1F))
                            .child(group.name.clone()),
                    ),
            )
            .child(self.render_type_badge(group.group_type))
    }

    /// Render type badge
    fn render_type_badge(&self, group_type: ConsumerType) -> Div {
        div().px_2().py_1().rounded(px(6.0)).bg(group_type.bg_color()).child(
            div()
                .text_xs()
                .font_weight(FontWeight::MEDIUM)
                .text_color(group_type.color())
                .child(group_type.display_name()),
        )
    }

    /// Render the card statistics
    fn render_card_stats(&self, group: &ConsumerGroupData) -> Div {
        div()
            .flex()
            .items_center()
            .gap(px(32.0))
            .child(self.render_stat("TPS:", &group.tps))
            .child(self.render_stat("Delay:", &group.delay))
            .child(self.render_stat("Clients:", &group.clients.to_string()))
    }

    /// Render a single stat
    fn render_stat(&self, label: &'static str, value: &str) -> Div {
        div()
            .flex()
            .items_center()
            .gap_1()
            .child(div().text_xs().text_color(rgb(0x86868B)).child(label))
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(rgb(0x1D1D1F))
                    .child(value.to_string()),
            )
    }

    /// Render the card action buttons
    fn render_card_actions(&self, group: &ConsumerGroupData, cx: &mut Context<Self>) -> Div {
        let is_system = group.group_type == ConsumerType::System;
        let group_index = self.consumer_groups.iter().position(|g| g.name == group.name).unwrap();

        if is_system {
            // System groups have 3 buttons in one row
            self.render_button_row_with_modal(
                &[("CLIENT", "\u{1F465}"), ("DETAIL", "\u{1F441}"), ("CONFIG", "\u{2699}")],
                group_index,
                cx,
            )
        } else {
            // Normal groups have 2 rows
            div()
                .flex()
                .flex_col()
                .gap_2()
                .w_full()
                .child(self.render_button_row_with_modal(
                    &[("CLIENT", "\u{1F465}"), ("DETAIL", "\u{1F441}")],
                    group_index,
                    cx,
                ))
                .child(
                    div()
                        .flex()
                        .items_center()
                        .gap_2()
                        .w_full()
                        .justify_between()
                        .child(self.render_action_button("CONFIG", "\u{2699}", rgb(0x1D1D1F), rgb(0xF5F5F7)))
                        .child(self.render_action_button("REFRESH", "\u{1F504}", rgb(0x1D1D1F), rgb(0xF5F5F7)))
                        .child(self.render_action_button("DELETE", "\u{1F5D1}", rgb(0xFF3B30), rgb(0xFFE5E5))),
                )
        }
    }

    /// Render a row of action buttons with modal trigger
    fn render_button_row_with_modal(
        &self,
        buttons: &[(&'static str, &'static str)],
        group_index: usize,
        cx: &mut Context<Self>,
    ) -> Div {
        div()
            .flex()
            .items_center()
            .gap_2()
            .w_full()
            .justify_between()
            .children(buttons.iter().map(|(text, icon)| {
                if *text == "DETAIL" {
                    self.render_detail_button(group_index, cx).into_any_element()
                } else {
                    self.render_action_button(text, icon, rgb(0x1D1D1F), rgb(0xF5F5F7))
                        .into_any_element()
                }
            }))
    }

    /// Render a detail button with click handler
    fn render_detail_button(&self, group_index: usize, cx: &mut Context<Self>) -> impl IntoElement {
        let is_selected = self.modal_consumer_index == Some(group_index);
        let button_id: SharedString = format!("detail-btn-{}", group_index).into();

        div()
            .id(button_id)
            .flex_1()
            .flex()
            .items_center()
            .justify_center()
            .gap_1()
            .px_2()
            .py_1()
            .rounded(px(8.0))
            .border_1()
            .border_color(if is_selected { rgb(0x007AFF) } else { rgb(0xE5E5E7) })
            .bg(if is_selected { rgb(0x007AFF) } else { rgb(0xF5F5F7) })
            .cursor_pointer()
            .on_click(cx.listener(move |this, _event, _window, cx| {
                // Toggle modal - if already selected, close it; otherwise open it
                if this.modal_consumer_index == Some(group_index) {
                    println!("Closing modal for group {}", group_index);
                    this.modal_consumer_index = None;
                } else {
                    println!("Opening modal for group {}", group_index);
                    this.modal_consumer_index = Some(group_index);
                }
                cx.notify();
            }))
            .child(
                div()
                    .text_xs()
                    .text_color(if is_selected { rgb(0xFFFFFF) } else { rgb(0x1D1D1F) })
                    .child("\u{1F441}"),
            )
            .child(
                div()
                    .text_xs()
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(if is_selected { rgb(0xFFFFFF) } else { rgb(0x1D1D1F) })
                    .child("DETAIL"),
            )
    }

    /// Render the close button for modal
    fn render_close_button(&self, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .id("modal-close-btn")
            .w(px(32.0))
            .h(px(32.0))
            .flex()
            .items_center()
            .justify_center()
            .rounded(px(8.0))
            .bg(rgb(0xF5F5F7))
            .cursor_pointer()
            .on_click(cx.listener(|this, _event, _window, cx| {
                this.modal_consumer_index = None;
                cx.notify();
            }))
            .child(div().text_lg().text_color(rgb(0x86868B)).child("\u{2715}"))
    }

    /// Render an action button
    fn render_action_button(&self, text: &'static str, icon: &'static str, text_color: Rgba, bg: Rgba) -> Div {
        div()
            .flex_1()
            .flex()
            .items_center()
            .justify_center()
            .gap_1()
            .px_2()
            .py_1()
            .rounded(px(8.0))
            .bg(bg)
            .cursor_pointer()
            .child(div().text_xs().text_color(text_color).child(icon.to_string()))
            .child(
                div()
                    .text_xs()
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(text_color)
                    .child(text),
            )
    }

    /// Render pagination section
    fn render_pagination(&self) -> Div {
        div()
            .flex()
            .items_center()
            .justify_between()
            .w_full()
            .child(
                div()
                    .text_sm()
                    .text_color(rgb(0x86868B))
                    .child("Showing 1-6 of 24 groups"),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_1()
                    .child(self.render_page_button("\u{2039}", false))
                    .child(self.render_page_button("1", true))
                    .child(self.render_page_button("2", false))
                    .child(self.render_page_button("3", false))
                    .child(self.render_page_button("\u{203A}", false)),
            )
    }

    /// Render a pagination button
    fn render_page_button(&self, text: &'static str, is_active: bool) -> Div {
        let bg = if is_active { rgb(0x007AFF) } else { rgb(0xF5F5F7) };
        let text_color = if is_active { rgb(0xFFFFFF) } else { rgb(0x1D1D1F) };

        div()
            .min_w(px(32.0))
            .h(px(32.0))
            .flex()
            .items_center()
            .justify_center()
            .rounded(px(6.0))
            .bg(bg)
            .cursor_pointer()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(text_color)
                    .child(text),
            )
    }

    /// Render just the modal content without backdrop (for use in render_page)
    fn render_modal_content_only(&self, cx: &mut Context<Self>) -> Div {
        if let Some(index) = self.modal_consumer_index {
            if let Some(group) = self.consumer_groups.get(index) {
                return div()
                    .absolute()
                    .inset_0()
                    .flex()
                    .items_center()
                    .justify_center()
                    .bg(rgba(0x000000AA))
                    .child(self.render_modal_content(group, cx));
            }
        }
        div()
    }

    /// Render the modal content for a specific consumer group
    fn render_modal_content(&self, group: &ConsumerGroupData, cx: &mut Context<Self>) -> Div {
        div()
            .w(px(800.0))
            .h(px(600.0))
            .rounded(px(16.0))
            .bg(rgb(0xFFFFFF))
            .border_1()
            .border_color(rgb(0xE5E5E7))
            .shadow_2xl()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(self.render_modal_header(group, cx))
            .child(self.render_modal_body(group))
    }

    /// Render the modal header
    fn render_modal_header(&self, group: &ConsumerGroupData, cx: &mut Context<Self>) -> Div {
        div()
            .flex()
            .items_center()
            .justify_between()
            .p_6()
            .border_b_1()
            .border_color(rgb(0xE5E5E7))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_4()
                    .child(
                        div()
                            .w(px(48.0))
                            .h(px(48.0))
                            .flex()
                            .items_center()
                            .justify_center()
                            .rounded(px(12.0))
                            .bg(group.group_type.bg_color())
                            .child(
                                div()
                                    .text_2xl()
                                    .text_color(group.group_type.color())
                                    .child(group.group_type.icon()),
                            ),
                    )
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .gap_1()
                            .child(
                                div()
                                    .text_xl()
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .text_color(rgb(0x1D1D1F))
                                    .child(group.name.clone()),
                            )
                            .child(
                                div()
                                    .text_sm()
                                    .text_color(rgb(0x86868B))
                                    .child(format!("Type: {}", group.group_type.display_name())),
                            ),
                    ),
            )
            .child(self.render_close_button(cx))
    }

    /// Render the modal body
    fn render_modal_body(&self, group: &ConsumerGroupData) -> Div {
        div()
            .flex()
            .flex_col()
            .gap_5()
            .p_6()
            .flex_1()
            .child(self.render_stats_section(group))
            .child(self.render_config_section(group))
            .child(self.render_clients_section())
    }

    /// Render the statistics section
    fn render_stats_section(&self, group: &ConsumerGroupData) -> Div {
        div()
            .flex()
            .flex_col()
            .gap_3()
            .child(
                div()
                    .text_base()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(rgb(0x1D1D1F))
                    .child("Statistics"),
            )
            .child(
                div().flex().gap_4().children(
                    [
                        ("TPS", group.tps.clone(), rgb(0x007AFF)),
                        ("Delay", group.delay.clone(), rgb(0x34C759)),
                        ("Clients", group.clients.to_string(), rgb(0xFF9500)),
                    ]
                    .iter()
                    .map(|(label, value, color)| {
                        div()
                            .flex_1()
                            .rounded(px(12.0))
                            .bg(rgb(0xF5F5F7))
                            .p_4()
                            .flex()
                            .flex_col()
                            .gap_2()
                            .child(div().text_sm().text_color(rgb(0x86868B)).child(*label))
                            .child(
                                div()
                                    .text_2xl()
                                    .font_weight(FontWeight::BOLD)
                                    .text_color(*color)
                                    .child(value.clone()),
                            )
                    }),
                ),
            )
    }

    /// Render the configuration section
    fn render_config_section(&self, _group: &ConsumerGroupData) -> Div {
        let configs = [
            ("Consume Model", "CLUSTERING"),
            ("Consume From Where", "CONSUME_FROM_LAST_OFFSET"),
            ("Consume Timeout", "15min"),
            ("Max Reconsume Times", "16"),
        ];

        div()
            .flex()
            .flex_col()
            .gap_3()
            .child(
                div()
                    .text_base()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(rgb(0x1D1D1F))
                    .child("Configuration"),
            )
            .child(
                div()
                    .rounded(px(12.0))
                    .bg(rgb(0xF5F5F7))
                    .p_4()
                    .flex()
                    .flex_col()
                    .gap_3()
                    .children(configs.iter().map(|(key, value)| {
                        div()
                            .flex()
                            .items_center()
                            .justify_between()
                            .child(div().text_sm().text_color(rgb(0x86868B)).child(*key))
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::MEDIUM)
                                    .text_color(rgb(0x1D1D1F))
                                    .child(*value),
                            )
                    })),
            )
    }

    /// Render the connected clients section
    fn render_clients_section(&self) -> Div {
        let clients = [
            ("192.168.1.100:10911", "Active", "2,340 TPS"),
            ("192.168.1.101:10911", "Active", "1,890 TPS"),
            ("192.168.1.102:10911", "Idle", "0 TPS"),
        ];

        div()
            .flex()
            .flex_col()
            .gap_3()
            .child(
                div()
                    .text_base()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(rgb(0x1D1D1F))
                    .child("Connected Clients"),
            )
            .child(
                div()
                    .rounded(px(12.0))
                    .bg(rgb(0xF5F5F7))
                    .p_4()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .children(clients.iter().map(|(addr, status, tps)| {
                        div()
                            .flex()
                            .items_center()
                            .justify_between()
                            .px_3()
                            .py_2()
                            .rounded(px(8.0))
                            .bg(rgb(0xFFFFFF))
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::MEDIUM)
                                    .text_color(rgb(0x1D1D1F))
                                    .child(*addr),
                            )
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap_3()
                                    .child(
                                        div()
                                            .px_2()
                                            .py_1()
                                            .rounded(px(4.0))
                                            .bg(if *status == "Active" {
                                                rgb(0xE8F5E9)
                                            } else {
                                                rgb(0xFFF3E0)
                                            })
                                            .child(
                                                div()
                                                    .text_xs()
                                                    .font_weight(FontWeight::MEDIUM)
                                                    .text_color(if *status == "Active" {
                                                        rgb(0x34C759)
                                                    } else {
                                                        rgb(0xFF9500)
                                                    })
                                                    .child(*status),
                                            ),
                                    )
                                    .child(div().text_xs().text_color(rgb(0x86868B)).child(*tps)),
                            )
                    })),
            )
    }
}

impl Render for ConsumerView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.render_page(cx)
    }
}

impl Default for ConsumerView {
    fn default() -> Self {
        Self::new()
    }
}
