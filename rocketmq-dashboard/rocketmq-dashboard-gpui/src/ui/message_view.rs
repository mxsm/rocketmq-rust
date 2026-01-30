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

//! Message Query view component
//!
//! This module provides the Message Query page with search functionality
//! for querying messages by topic, message key, message ID, and time range.

use gpui::prelude::FluentBuilder;
use gpui::*;

/// Message data model
struct MessageData {
    msg_id: String,
    topic: String,
    tags: String,
    keys: String,
    queue_id: u32,
    queue_offset: u64,
    store_size: u32,
    born_timestamp: String,
    born_host: String,
    store_timestamp: String,
    store_host: String,
    msg_type: MessageType,
    icon_color: Rgba,
    icon_bg: Rgba,
}

/// Message type enumeration
#[derive(Clone, Copy, Debug)]
enum MessageType {
    Normal,
    Delay,
    Transaction,
}

impl MessageType {
    fn display_name(&self) -> &'static str {
        match self {
            MessageType::Normal => "NORMAL",
            MessageType::Delay => "DELAY",
            MessageType::Transaction => "TRANSACTION",
        }
    }

    fn color(&self) -> Rgba {
        match self {
            MessageType::Normal => rgb(0x34C759),
            MessageType::Delay => rgb(0xFF9500),
            MessageType::Transaction => rgb(0x007AFF),
        }
    }

    fn bg_color(&self) -> Rgba {
        match self {
            MessageType::Normal => rgb(0xE8F5E9),
            MessageType::Delay => rgb(0xFFF3E0),
            MessageType::Transaction => rgb(0xE3F2FD),
        }
    }
}

impl MessageData {
    fn new(
        msg_id: &str,
        topic: &str,
        tags: &str,
        keys: &str,
        queue_id: u32,
        queue_offset: u64,
        store_size: u32,
        born_timestamp: &str,
        born_host: &str,
        store_timestamp: &str,
        store_host: &str,
        msg_type: MessageType,
        icon_color: Rgba,
        icon_bg: Rgba,
    ) -> Self {
        Self {
            msg_id: msg_id.to_string(),
            topic: topic.to_string(),
            tags: tags.to_string(),
            keys: keys.to_string(),
            queue_id,
            queue_offset,
            store_size,
            born_timestamp: born_timestamp.to_string(),
            born_host: born_host.to_string(),
            store_timestamp: store_timestamp.to_string(),
            store_host: store_host.to_string(),
            msg_type,
            icon_color,
            icon_bg,
        }
    }
}

/// Message Query view
pub struct MessageView {
    /// Selected topic
    selected_topic: String,
    /// Message key input
    message_key_input: String,
    /// Message ID input
    message_id_input: String,
    /// Selected time range
    selected_time_range: String,
    /// Message list data
    messages: Vec<MessageData>,
    /// Index of message whose detail modal should be shown
    modal_message_index: Option<usize>,
}

impl MessageView {
    /// Create a new Message Query view
    pub fn new() -> Self {
        // Sample message data
        let messages = vec![
            MessageData::new(
                "C0A8017E00002A9F0000000000001A3F",
                "TopicTest",
                "tagA",
                "key-123456",
                0,
                1245,
                256,
                "2026-01-28 12:34:56",
                "192.168.192.1:12345",
                "2026-01-28 12:34:57",
                "192.168.192.1:10911",
                MessageType::Normal,
                rgb(0x007AFF),
                rgb(0xE3F2FD),
            ),
            MessageData::new(
                "C0A8017E00002A9F0000000000001A40",
                "TopicTest",
                "tagB",
                "key-789012",
                1,
                1246,
                320,
                "2026-01-28 12:35:01",
                "192.168.192.2:54321",
                "2026-01-28 12:35:02",
                "192.168.192.1:10911",
                MessageType::Transaction,
                rgb(0x34C759),
                rgb(0xE8F5E9),
            ),
            MessageData::new(
                "C0A8017E00002A9F0000000000001A41",
                "TopicTest",
                "tagC",
                "key-345678",
                0,
                1247,
                180,
                "2026-01-28 12:36:15",
                "192.168.192.3:98765",
                "2026-01-28 12:36:16",
                "192.168.192.1:10911",
                MessageType::Delay,
                rgb(0xFF9500),
                rgb(0xFFF3E0),
            ),
        ];

        Self {
            selected_topic: "TopicTest".to_string(),
            message_key_input: String::new(),
            message_id_input: String::new(),
            selected_time_range: "Last 1 Hour".to_string(),
            messages,
            modal_message_index: None,
        }
    }

    /// Render the complete message query page
    fn render_page(&self, cx: &mut Context<Self>) -> Div {
        div()
            .size_full()
            .flex()
            .flex_col()
            .relative()
            .child(self.render_main_content(cx))
            .when(self.modal_message_index.is_some(), |parent| {
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
            .child(self.render_search_bar(cx))
            .child(self.render_message_list(cx))
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
                    .child("Message Query"),
            )
            .child(
                div()
                    .text_base()
                    .text_color(rgb(0x98989D))
                    .child("Search and view RocketMQ messages by topic, key, or ID"),
            )
    }

    /// Render the search bar
    fn render_search_bar(&self, cx: &mut Context<Self>) -> Div {
        div()
            .w_full()
            .rounded(px(12.0))
            .bg(rgb(0xFFFFFF))
            .border_1()
            .border_color(rgb(0xE5E5E7))
            .p_6()
            .flex()
            .flex_col()
            .gap_5()
            .child(self.render_search_fields(cx))
    }

    /// Render the search fields
    fn render_search_fields(&self, cx: &mut Context<Self>) -> Div {
        div()
            .w_full()
            .flex()
            .items_end()
            .gap_5()
            .child(self.render_topic_field())
            .child(self.render_message_key_field())
            .child(self.render_message_id_field())
            .child(self.render_time_range_field())
            .child(self.render_search_button(cx))
    }

    /// Render topic dropdown field
    fn render_topic_field(&self) -> Div {
        div()
            .flex()
            .flex_col()
            .gap_2()
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .text_color(rgb(0x1D1D1F))
                            .child("* TOPIC:"),
                    )
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .text_color(rgb(0xFF3B30))
                            .child("*"),
                    ),
            )
            .child(
                div()
                    .w(px(200.0))
                    .h(px(40.0))
                    .flex()
                    .items_center()
                    .justify_between()
                    .px_3()
                    .rounded(px(8.0))
                    .bg(rgb(0xF5F5F7))
                    .border_1()
                    .border_color(rgb(0xE5E5E7))
                    .child(
                        div()
                            .text_sm()
                            .text_color(rgb(0x1D1D1F))
                            .child(self.selected_topic.clone()),
                    )
                    .child(
                        div().text_base().text_color(rgb(0x86868B)).child("\u{E313}"), // expand_more icon
                    ),
            )
    }

    /// Render message key input field
    fn render_message_key_field(&self) -> Div {
        div()
            .flex()
            .flex_col()
            .gap_2()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(rgb(0x1D1D1F))
                    .child("MESSAGE KEY:"),
            )
            .child(
                div()
                    .w(px(200.0))
                    .h(px(40.0))
                    .flex()
                    .items_center()
                    .px_3()
                    .rounded(px(8.0))
                    .bg(rgb(0xF5F5F7))
                    .border_1()
                    .border_color(rgb(0xE5E5E7))
                    .child(
                        div()
                            .text_sm()
                            .text_color(rgb(0x1D1D1F))
                            .child(if self.message_key_input.is_empty() {
                                "Enter message key".to_string()
                            } else {
                                self.message_key_input.clone()
                            }),
                    ),
            )
    }

    /// Render message ID input field
    fn render_message_id_field(&self) -> Div {
        div()
            .flex()
            .flex_col()
            .gap_2()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(rgb(0x1D1D1F))
                    .child("MESSAGE ID:"),
            )
            .child(
                div()
                    .w(px(300.0))
                    .h(px(40.0))
                    .flex()
                    .items_center()
                    .px_3()
                    .rounded(px(8.0))
                    .bg(rgb(0xF5F5F7))
                    .border_1()
                    .border_color(rgb(0xE5E5E7))
                    .child(
                        div()
                            .text_sm()
                            .text_color(rgb(0x1D1D1F))
                            .child(if self.message_id_input.is_empty() {
                                "Enter message ID".to_string()
                            } else {
                                self.message_id_input.clone()
                            }),
                    ),
            )
    }

    /// Render time range dropdown field
    fn render_time_range_field(&self) -> Div {
        div()
            .flex()
            .flex_col()
            .gap_2()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(rgb(0x1D1D1F))
                    .child("TIME RANGE:"),
            )
            .child(
                div()
                    .w(px(180.0))
                    .h(px(40.0))
                    .flex()
                    .items_center()
                    .justify_between()
                    .px_3()
                    .rounded(px(8.0))
                    .bg(rgb(0xF5F5F7))
                    .border_1()
                    .border_color(rgb(0xE5E5E7))
                    .child(
                        div()
                            .text_sm()
                            .text_color(rgb(0x1D1D1F))
                            .child(self.selected_time_range.clone()),
                    )
                    .child(
                        div().text_base().text_color(rgb(0x86868B)).child("\u{E313}"), // expand_more icon
                    ),
            )
    }

    /// Render search button
    fn render_search_button(&self, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .id("search-btn")
            .flex()
            .items_center()
            .gap(px(6.0))
            .px_5()
            .py(px(10.0))
            .rounded(px(8.0))
            .bg(rgb(0x007AFF))
            .cursor_pointer()
            .on_click(cx.listener(|this, _event, _window, cx| {
                println!(
                    "Search messages - Topic: {}, Key: {}, ID: {}",
                    this.selected_topic, this.message_key_input, this.message_id_input
                );
                cx.notify();
            }))
            .child(
                div().text_base().text_color(rgb(0xFFFFFF)).child("\u{1F50D}"), // search icon 🔍
            )
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(rgb(0xFFFFFF))
                    .child("SEARCH"),
            )
    }

    /// Render the message list section
    fn render_message_list(&self, cx: &mut Context<Self>) -> Div {
        div()
            .flex()
            .flex_col()
            .gap_6()
            .w_full()
            .flex_1()
            .child(self.render_message_list_header(self.messages.len()))
            .child(self.render_message_grid(cx))
    }

    /// Render the message list header
    fn render_message_list_header(&self, count: usize) -> Div {
        div()
            .flex()
            .items_center()
            .gap_4()
            .w_full()
            .child(
                div()
                    .text_xl()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(rgb(0x1D1D1F))
                    .child("Messages"),
            )
            .child(
                div().px_3().py_1().rounded(px(12.0)).bg(rgb(0xF5F5F7)).child(
                    div()
                        .text_sm()
                        .font_weight(FontWeight::MEDIUM)
                        .text_color(rgb(0x86868B))
                        .child(count.to_string()),
                ),
            )
    }

    /// Render the message cards grid (one per row for message details)
    fn render_message_grid(&self, cx: &mut Context<Self>) -> Div {
        div()
            .flex()
            .flex_col()
            .gap_4()
            .w_full()
            .children(self.messages.iter().map(|msg| self.render_message_card(msg, cx)))
    }

    /// Render a single message card
    fn render_message_card(&self, msg: &MessageData, cx: &mut Context<Self>) -> Div {
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
            .child(self.render_card_header(msg))
            .child(self.render_card_details(msg))
            .child(div().h(px(1.0)).bg(rgb(0xE5E5E7)))
            .child(self.render_card_actions(msg, cx))
    }

    /// Render the card header
    fn render_card_header(&self, msg: &MessageData) -> Div {
        div()
            .flex()
            .items_center()
            .justify_between()
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
                            .bg(msg.icon_bg)
                            .child(
                                div().text_xl().text_color(msg.icon_color).child("\u{1F4AC}"), // message icon emoji 💬
                            ),
                    )
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .gap_1()
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap_3()
                                    .child(
                                        div()
                                            .text_base()
                                            .font_weight(FontWeight::MEDIUM)
                                            .text_color(rgb(0x1D1D1F))
                                            .child(msg.msg_id.clone()),
                                    )
                                    .child(self.render_type_badge(msg.msg_type)),
                            )
                            .child(div().text_sm().text_color(rgb(0x86868B)).child(msg.topic.clone())),
                    ),
            )
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(rgb(0x86868B))
                    .child(format!("Q{} / Offset: {}", msg.queue_id, msg.queue_offset)),
            )
    }

    /// Render message type badge
    fn render_type_badge(&self, msg_type: MessageType) -> Div {
        div().px_2().py_1().rounded(px(6.0)).bg(msg_type.bg_color()).child(
            div()
                .text_xs()
                .font_weight(FontWeight::MEDIUM)
                .text_color(msg_type.color())
                .child(msg_type.display_name()),
        )
    }

    /// Render the card details
    fn render_card_details(&self, msg: &MessageData) -> Div {
        div()
            .flex()
            .flex_col()
            .gap_3()
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_5()
                    .child(self.render_detail_item("Tags", &msg.tags))
                    .child(self.render_detail_item("Keys", &msg.keys))
                    .child(self.render_detail_item("Size", &format!("{} B", msg.store_size))),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_5()
                    .child(self.render_detail_item("Born Time", &msg.born_timestamp))
                    .child(self.render_detail_item("Born Host", &msg.born_host))
                    .child(self.render_detail_item("Store Time", &msg.store_timestamp))
                    .child(self.render_detail_item("Store Host", &msg.store_host)),
            )
    }

    /// Render a single detail item
    fn render_detail_item(&self, label: &str, value: &str) -> Div {
        div()
            .flex()
            .flex_col()
            .gap_1()
            .child(div().text_xs().text_color(rgb(0x86868B)).child(label.to_string()))
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(rgb(0x1D1D1F))
                    .child(value.to_string()),
            )
    }

    /// Render the card action buttons
    fn render_card_actions(&self, msg: &MessageData, cx: &mut Context<Self>) -> Div {
        let message_index = self.messages.iter().position(|m| m.msg_id == msg.msg_id).unwrap();

        div()
            .flex()
            .items_center()
            .gap(px(10.0))
            .w_full()
            .child(self.render_detail_button(message_index, cx))
            .child(self.render_action_button("\u{1F4BE}", "Save", rgb(0x1D1D1F), rgb(0xF5F5F7)))
            .child(self.render_action_button("\u{1F512}", "Verify", rgb(0x007AFF), rgb(0xE3F2FD)))
    }

    /// Render a detail button with click handler
    fn render_detail_button(&self, message_index: usize, cx: &mut Context<Self>) -> impl IntoElement {
        let is_selected = self.modal_message_index == Some(message_index);
        let button_id: SharedString = format!("detail-btn-{}", message_index).into();

        div()
            .id(button_id)
            .flex_1()
            .flex()
            .items_center()
            .justify_center()
            .gap(px(6.0))
            .px(px(14.0))
            .py_2()
            .rounded(px(8.0))
            .border_1()
            .border_color(if is_selected { rgb(0x007AFF) } else { rgb(0xE5E5E7) })
            .bg(if is_selected { rgb(0x007AFF) } else { rgb(0xF5F5F7) })
            .cursor_pointer()
            .on_click(cx.listener(move |this, _event, _window, cx| {
                if this.modal_message_index == Some(message_index) {
                    println!("Closing modal for message {}", message_index);
                    this.modal_message_index = None;
                } else {
                    println!("Opening modal for message {}", message_index);
                    this.modal_message_index = Some(message_index);
                }
                cx.notify();
            }))
            .child(
                div()
                    .text_sm()
                    .text_color(if is_selected { rgb(0xFFFFFF) } else { rgb(0x1D1D1F) })
                    .child("\u{1F441}"), // visibility icon 👁
            )
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(if is_selected { rgb(0xFFFFFF) } else { rgb(0x1D1D1F) })
                    .child("Detail"),
            )
    }

    /// Render an action button
    fn render_action_button(&self, icon: &'static str, text: &'static str, text_color: Rgba, bg: Rgba) -> Div {
        div()
            .flex_1()
            .flex()
            .items_center()
            .justify_center()
            .gap(px(6.0))
            .px(px(14.0))
            .py_2()
            .rounded(px(8.0))
            .bg(bg)
            .cursor_pointer()
            .child(div().text_sm().text_color(text_color).child(icon.to_string()))
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
        if let Some(index) = self.modal_message_index {
            if let Some(msg) = self.messages.get(index) {
                return div()
                    .absolute()
                    .inset_0()
                    .flex()
                    .items_center()
                    .justify_center()
                    .bg(rgba(0x000000AA))
                    .child(self.render_modal_content(msg, cx));
            }
        }
        div()
    }

    /// Render the modal content for a specific message
    fn render_modal_content(&self, msg: &MessageData, cx: &mut Context<Self>) -> Div {
        div()
            .w(px(900.0))
            .max_h(px(80.0))
            .rounded(px(16.0))
            .bg(rgb(0xFFFFFF))
            .border_1()
            .border_color(rgb(0xE5E5E7))
            .shadow_2xl()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(self.render_modal_header(msg, cx))
            .child(self.render_modal_body(msg))
    }

    /// Render the modal header
    fn render_modal_header(&self, msg: &MessageData, cx: &mut Context<Self>) -> Div {
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
                            .bg(msg.icon_bg)
                            .child(
                                div().text_2xl().text_color(msg.icon_color).child("\u{1F4AC}"), /* message icon
                                                                                                 * emoji 💬 */
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
                                    .child("Message Details"),
                            )
                            .child(div().text_sm().text_color(rgb(0x86868B)).child(msg.msg_id.clone())),
                    ),
            )
            .child(self.render_close_button(cx))
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
                this.modal_message_index = None;
                cx.notify();
            }))
            .child(div().text_lg().text_color(rgb(0x86868B)).child("\u{2715}"))
    }

    /// Render the modal body
    fn render_modal_body(&self, msg: &MessageData) -> Div {
        div()
            .flex()
            .flex_col()
            .gap_5()
            .p_6()
            .flex_1()
            .child(self.render_basic_info_section(msg))
            .child(self.render_route_info_section(msg))
            .child(self.render_message_content_section())
    }

    /// Render the basic information section
    fn render_basic_info_section(&self, msg: &MessageData) -> Div {
        let info = [
            ("Message ID", msg.msg_id.clone()),
            ("Topic", msg.topic.clone()),
            ("Tags", msg.tags.clone()),
            ("Keys", msg.keys.clone()),
            ("Queue ID", msg.queue_id.to_string()),
            ("Queue Offset", msg.queue_offset.to_string()),
            ("Store Size", format!("{} bytes", msg.store_size)),
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
                    .child("Basic Information"),
            )
            .child(
                div()
                    .rounded(px(12.0))
                    .bg(rgb(0xF5F5F7))
                    .p_4()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .children(info.iter().map(|(key, value)| {
                        div()
                            .flex()
                            .items_center()
                            .justify_between()
                            .child(div().text_sm().text_color(rgb(0x86868B)).child((*key).to_string()))
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::MEDIUM)
                                    .text_color(rgb(0x1D1D1F))
                                    .child(value.clone()),
                            )
                    })),
            )
    }

    /// Render the route information section
    fn render_route_info_section(&self, msg: &MessageData) -> Div {
        let route_info = [
            ("Born Timestamp", msg.born_timestamp.clone()),
            ("Born Host", msg.born_host.clone()),
            ("Store Timestamp", msg.store_timestamp.clone()),
            ("Store Host", msg.store_host.clone()),
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
                    .child("Route Information"),
            )
            .child(
                div()
                    .rounded(px(12.0))
                    .bg(rgb(0xF5F5F7))
                    .p_4()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .children(route_info.iter().map(|(key, value)| {
                        div()
                            .flex()
                            .items_center()
                            .justify_between()
                            .child(div().text_sm().text_color(rgb(0x86868B)).child((*key).to_string()))
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::MEDIUM)
                                    .text_color(rgb(0x1D1D1F))
                                    .child(value.clone()),
                            )
                    })),
            )
    }

    /// Render the message content section
    fn render_message_content_section(&self) -> Div {
        div()
            .flex()
            .flex_col()
            .gap_3()
            .child(
                div()
                    .text_base()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(rgb(0x1D1D1F))
                    .child("Message Content"),
            )
            .child(
                div()
                    .rounded(px(12.0))
                    .bg(rgb(0xF5F5F7))
                    .p_4()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .text_color(rgb(0x1D1D1F))
                            .child("{\n  \"messageId\": \"C0A8017E00002A9F0000000000001A3F\",\n  \"topic\": \"TopicTest\",\n  \"content\": \"Sample message content\",\n  \"tags\": \"tagA\",\n  \"keys\": \"key-123456\",\n  \"timestamp\": 1706450097000\n}"),
                    ),
            )
    }
}

impl Render for MessageView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.render_page(cx)
    }
}

impl Default for MessageView {
    fn default() -> Self {
        Self::new()
    }
}
