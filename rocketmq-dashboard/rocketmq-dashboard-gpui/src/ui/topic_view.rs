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

//! Topic Management view component
//!
//! This module provides the Topic Management page with filtering,
//! topic cards grid, and operations.

use gpui::*;

/// Topic type classification
#[derive(Clone, Copy, Debug, PartialEq)]
enum TopicType {
    Normal,
    Delay,
    Fifo,
    Transaction,
    Unspecified,
    Retry,
    Dlq,
    System,
}

impl TopicType {
    fn display_name(&self) -> &'static str {
        match self {
            TopicType::Normal => "Normal",
            TopicType::Delay => "Delay",
            TopicType::Fifo => "FIFO",
            TopicType::Transaction => "Transaction",
            TopicType::Unspecified => "Unspecified",
            TopicType::Retry => "Retry",
            TopicType::Dlq => "DLQ",
            TopicType::System => "System",
        }
    }

    fn color(&self) -> Rgba {
        match self {
            TopicType::Normal => rgb(0x007AFF),
            TopicType::Delay => rgb(0xFF9500),
            TopicType::Fifo => rgb(0x34C759),
            TopicType::Transaction => rgb(0xAF52DE),
            TopicType::Unspecified => rgb(0x8E8E93),
            TopicType::Retry => rgb(0xFF3B30),
            TopicType::Dlq => rgb(0xFFCC00),
            TopicType::System => rgb(0x5856D6),
        }
    }
}

/// Topic data model
struct TopicData {
    name: String,
    topic_type: TopicType,
    queues: u32,
    partitions: u32,
    tps: String,
    messages: String,
}

impl TopicData {
    fn new(name: &str, topic_type: TopicType, queues: u32, partitions: u32, tps: &str, messages: &str) -> Self {
        Self {
            name: name.to_string(),
            topic_type,
            queues,
            partitions,
            tps: tps.to_string(),
            messages: messages.to_string(),
        }
    }
}

/// Topic Management view
pub struct TopicView {
    /// Active filter types
    active_filters: Vec<TopicType>,
    /// Topic list data
    topics: Vec<TopicData>,
}

impl TopicView {
    /// Create a new Topic Management view
    pub fn new() -> Self {
        // Initialize with all filters active
        let active_filters = vec![
            TopicType::Normal,
            TopicType::Delay,
            TopicType::Fifo,
            TopicType::Transaction,
            TopicType::Unspecified,
            TopicType::Retry,
            TopicType::Dlq,
            TopicType::System,
        ];

        // Sample topic data
        let topics = vec![
            TopicData::new("broker-mxsm-a", TopicType::Normal, 8, 12, "1.2K", "8.5M"),
            TopicData::new("%RETRY%mxsm-a", TopicType::Retry, 4, 8, "0.8K", "2.3M"),
            TopicData::new("OrderTopic", TopicType::Fifo, 16, 24, "2.1K", "12.4M"),
            TopicData::new("DelayTopicTest", TopicType::Delay, 6, 10, "0.5K", "1.8M"),
            TopicData::new("TransactionTopic", TopicType::Transaction, 8, 16, "1.5K", "6.7M"),
            TopicData::new("SysTopic", TopicType::System, 4, 6, "0.3K", "0.9M"),
        ];

        Self { active_filters, topics }
    }

    /// Render the complete topic management page
    fn render_page(&self, cx: &mut Context<Self>) -> Div {
        self.render_main_content(cx)
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
            .child(self.render_topic_list(cx))
    }

    /// Render the page header
    fn render_page_header() -> Div {
        div()
            .flex()
            .flex_col()
            .gap_2()
            .child(
                div()
                    .text_2xl()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(rgb(0x1D1D1F))
                    .child("Topic Management"),
            )
            .child(
                div()
                    .text_base()
                    .text_color(rgb(0x98989D))
                    .child("Manage and monitor your RocketMQ topics"),
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
    fn render_filter_header(&self, cx: &mut Context<Self>) -> Div {
        div()
            .flex()
            .items_center()
            .gap_4()
            .w_full()
            .child(
                div()
                    .w(px(320.0))
                    .h(px(38.0))
                    .flex()
                    .items_center()
                    .gap_3()
                    .px_4()
                    .rounded(px(8.0))
                    .bg(rgb(0xF5F5F7))
                    .border_1()
                    .border_color(rgb(0xE5E5E7))
                    .child(div().text_lg().text_color(rgb(0x86868B)).child("\u{1F50D}"))
                    .child(div().text_sm().text_color(rgb(0x98989D)).child("Search topics...")),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .flex_1()
                    .justify_end()
                    .child(self.render_button("refresh", "Refresh", rgb(0x1D1D1F), false, cx))
                    .child(self.render_button("add", "Add Topic", rgb(0xFFFFFF), true, cx)),
            )
    }

    /// Render a button
    fn render_button(
        &self,
        icon: &'static str,
        text: &'static str,
        text_color: Rgba,
        is_primary: bool,
        _cx: &mut Context<Self>,
    ) -> Div {
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
    fn render_filter_checkboxes(&self, cx: &mut Context<Self>) -> Div {
        let topic_types = [
            TopicType::Normal,
            TopicType::Delay,
            TopicType::Fifo,
            TopicType::Transaction,
            TopicType::Unspecified,
            TopicType::Retry,
            TopicType::Dlq,
            TopicType::System,
        ];

        div()
            .flex()
            .flex_wrap()
            .gap_2()
            .children(topic_types.iter().map(|topic_type| {
                let is_active = self.active_filters.contains(topic_type);
                self.render_filter_checkbox(topic_type, is_active, cx)
            }))
    }

    /// Render a filter checkbox
    fn render_filter_checkbox(
        &self,
        topic_type: &TopicType,
        is_active: bool,
        _cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let bg = if is_active { rgb(0xE3F2FD) } else { rgb(0xF5F5F7) };
        let text_color = if is_active { topic_type.color() } else { rgb(0x86868B) };

        div()
            .flex()
            .items_center()
            .gap_2()
            .px(px(14.0))
            .py_2()
            .rounded(px(6.0))
            .bg(bg)
            .cursor_pointer()
            .child(
                div()
                    .w(px(16.0))
                    .h(px(16.0))
                    .rounded(px(8.0))
                    .bg(if is_active { topic_type.color() } else { rgb(0xFFFFFF) })
                    .border_1()
                    .border_color(rgb(0xE5E5E7)),
            )
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(text_color)
                    .child(topic_type.display_name()),
            )
    }

    /// Render the topic list section
    fn render_topic_list(&self, cx: &mut Context<Self>) -> Div {
        div()
            .flex()
            .flex_col()
            .gap_6()
            .w_full()
            .flex_1()
            .child(self.render_topic_list_header())
            .child(self.render_topic_grid(cx))
    }

    /// Render the topic list header
    fn render_topic_list_header(&self) -> Div {
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
                    .child("Topics (192)"),
            )
            .child(div().flex_1())
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .px_4()
                    .py_2()
                    .rounded(px(8.0))
                    .bg(rgb(0xF5F5F7))
                    .cursor_pointer()
                    .child(div().text_lg().text_color(rgb(0x86868B)).child("\u{1F50D}"))
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .text_color(rgb(0x86868B))
                            .child("Name"),
                    ),
            )
    }

    /// Render the topic cards grid
    fn render_topic_grid(&self, cx: &mut Context<Self>) -> Div {
        // Filter topics based on active filters
        let filtered_topics: Vec<_> = self
            .topics
            .iter()
            .filter(|topic| self.active_filters.contains(&topic.topic_type))
            .collect();

        // Create grid rows with 3 cards each
        let rows: Vec<_> = filtered_topics.chunks(3).collect();

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
                    .children(row.iter().map(|topic| self.render_topic_card(topic, cx)))
            }))
    }

    /// Render a single topic card
    fn render_topic_card(&self, topic: &TopicData, _cx: &mut Context<Self>) -> Div {
        let icon_bg = self.get_topic_type_bg_color(topic.topic_type);

        div()
            .flex_1()
            .min_w(px(280.0))
            .rounded(px(16.0))
            .bg(rgb(0xFFFFFF))
            .border_1()
            .border_color(rgb(0xE5E5E7))
            .p_5()
            .flex()
            .flex_col()
            .gap_4()
            .child(self.render_card_header(topic, icon_bg))
            .child(self.render_card_actions(topic))
    }

    /// Get background color for topic type icon
    fn get_topic_type_bg_color(&self, topic_type: TopicType) -> Rgba {
        match topic_type {
            TopicType::Normal => rgb(0xE3F2FD),
            TopicType::Retry => rgb(0xFFE5E5),
            TopicType::Fifo => rgb(0xE8F5E9),
            TopicType::Delay => rgb(0xFFF3E0),
            TopicType::Transaction => rgb(0xF3E5F5),
            TopicType::System => rgb(0xE1F5FE),
            TopicType::Dlq => rgb(0xFFEBEE),
            TopicType::Unspecified => rgb(0xF5F5F7),
        }
    }

    /// Render the card header
    fn render_card_header(&self, topic: &TopicData, icon_bg: Rgba) -> Div {
        div()
            .flex()
            .items_center()
            .gap_4()
            .w_full()
            .child(
                div()
                    .w(px(56.0))
                    .h(px(56.0))
                    .flex()
                    .items_center()
                    .justify_center()
                    .rounded(px(14.0))
                    .bg(icon_bg)
                    .child(div().text_3xl().text_color(topic.topic_type.color()).child("\u{1F4C1}")),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .flex_1()
                    .child(
                        div()
                            .text_base()
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(rgb(0x1D1D1F))
                            .child(topic.name.clone()),
                    )
                    .child(div().text_sm().text_color(rgb(0x86868B)).child(format!(
                        "{} • {} queues • {} partitions",
                        topic.topic_type.display_name(),
                        topic.queues,
                        topic.partitions
                    )))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_3()
                            .child(
                                div()
                                    .text_xs()
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .text_color(rgb(0x34C759))
                                    .child(topic.tps.clone()),
                            )
                            .child(div().text_xs().text_color(rgb(0x86868B)).child("TPS"))
                            .child(div().text_xs().text_color(rgb(0x86868B)).child(topic.messages.clone()))
                            .child(div().text_xs().text_color(rgb(0x86868B)).child("Msg")),
                    ),
            )
    }

    /// Render the card action buttons
    fn render_card_actions(&self, _topic: &TopicData) -> Div {
        let row1_buttons = [
            ("Status", "\u{1F4A1}"),
            ("Route", "\u{1F5D0}"),
            ("Cons", "\u{1F465}"),
            ("Cfg", "\u{2699}"),
        ];

        let row2_buttons = [
            ("Send", "\u{1F4E4}"),
            ("Reset", "\u{1F504}"),
            ("Skip", "\u{23E9}"),
            ("Del", "\u{1F5D1}"),
        ];

        div()
            .flex()
            .flex_col()
            .gap_1()
            .w_full()
            .child(
                div().flex().items_center().gap_2().w_full().justify_between().children(
                    row1_buttons
                        .iter()
                        .map(|(text, icon)| self.render_action_button(text, icon, rgb(0x1D1D1F), rgb(0xF5F5F7))),
                ),
            )
            .child(
                div().flex().items_center().gap_2().w_full().justify_between().children(
                    row2_buttons
                        .iter()
                        .map(|(text, icon)| self.render_action_button(text, icon, rgb(0xFFFFFF), rgb(0xFF3B30))),
                ),
            )
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
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(text_color)
                    .child(text),
            )
    }
}

impl Render for TopicView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.render_page(cx)
    }
}

impl Default for TopicView {
    fn default() -> Self {
        Self::new()
    }
}
