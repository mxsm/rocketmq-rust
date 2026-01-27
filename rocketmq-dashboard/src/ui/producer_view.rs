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

//! Producer Client Management view component
//!
//! This module provides the Producer Client Management page with search,
//! filtering, and producer client cards with operations.

use gpui::prelude::FluentBuilder;
use gpui::*;

/// Producer client data model
struct ProducerClientData {
    client_id: String,
    producer_group: String,
    topic: String,
    address: String,
    connection_id: String,
    language: String,
    version: String,
    status: ProducerStatus,
    icon_color: Rgba,
    icon_bg: Rgba,
}

/// Producer connection status
#[derive(Clone, Copy, Debug)]
enum ProducerStatus {
    Active,
}

impl ProducerStatus {
    fn display_name(&self) -> &'static str {
        match self {
            ProducerStatus::Active => "ACTIVE",
        }
    }

    fn color(&self) -> Rgba {
        match self {
            ProducerStatus::Active => rgb(0x34C759),
        }
    }

    fn bg_color(&self) -> Rgba {
        match self {
            ProducerStatus::Active => rgb(0xE8F5E9),
        }
    }
}

impl ProducerClientData {
    fn new(
        client_id: &str,
        producer_group: &str,
        topic: &str,
        address: &str,
        connection_id: &str,
        language: &str,
        version: &str,
        status: ProducerStatus,
        icon_color: Rgba,
        icon_bg: Rgba,
    ) -> Self {
        Self {
            client_id: client_id.to_string(),
            producer_group: producer_group.to_string(),
            topic: topic.to_string(),
            address: address.to_string(),
            connection_id: connection_id.to_string(),
            language: language.to_string(),
            version: version.to_string(),
            status,
            icon_color,
            icon_bg,
        }
    }
}

/// Producer Management view
pub struct ProducerView {
    /// Selected topic for filtering
    selected_topic: String,
    /// Producer group search input
    producer_group_input: String,
    /// Producer client list data
    producer_clients: Vec<ProducerClientData>,
    /// Index of producer whose detail modal should be shown
    modal_producer_index: Option<usize>,
}

impl ProducerView {
    /// Create a new Producer Management view
    pub fn new() -> Self {
        // Sample producer client data
        let producer_clients = vec![
            ProducerClientData::new(
                "192.168.192.1@44096",
                "please_rename_unique_group_name",
                "TopicTest",
                "192.168.192.1:64339",
                "530745853933600",
                "JAVA",
                "V5_4_0",
                ProducerStatus::Active,
                rgb(0x007AFF),
                rgb(0xE3F2FD),
            ),
            ProducerClientData::new(
                "192.168.192.2@45001",
                "please_rename_unique_group_name",
                "TopicTest",
                "192.168.192.2:65231",
                "820745853933712",
                "PYTHON",
                "V5_3_0",
                ProducerStatus::Active,
                rgb(0x34C759),
                rgb(0xE8F5E9),
            ),
        ];

        Self {
            selected_topic: "TopicTest".to_string(),
            producer_group_input: "please_rename_unique_group_name".to_string(),
            producer_clients,
            modal_producer_index: None,
        }
    }

    /// Render the complete producer management page
    fn render_page(&self, cx: &mut Context<Self>) -> Div {
        div()
            .size_full()
            .flex()
            .flex_col()
            .relative()
            .child(self.render_main_content(cx))
            .when(self.modal_producer_index.is_some(), |parent| {
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
            .child(self.render_producer_list(cx))
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
                    .child("Producer Client Management"),
            )
            .child(
                div()
                    .text_base()
                    .text_color(rgb(0x98989D))
                    .child("Manage and monitor your RocketMQ producer clients"),
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
    fn render_search_fields(&self, _cx: &mut Context<Self>) -> Div {
        div()
            .w_full()
            .flex()
            .items_center()
            .gap_5()
            .child(self.render_topic_field())
            .child(self.render_group_field())
            .child(self.render_search_button())
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
                    .w(px(280.0))
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
                        div()
                            .text_base()
                            .text_color(rgb(0x86868B))
                            .child("\u{E313}"), // expand_more icon
                    ),
            )
    }

    /// Render producer group input field
    fn render_group_field(&self) -> Div {
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
                            .child("* PRODUCER_GROUP:"),
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
                    .w(px(320.0))
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
                            .child(self.producer_group_input.clone()),
                    ),
            )
    }

    /// Render search button
    fn render_search_button(&self) -> Div {
        div()
            .flex()
            .items_center()
            .gap(px(6.0))
            .px_5()
            .py(px(10.0))
            .rounded(px(8.0))
            .bg(rgb(0x007AFF))
            .cursor_pointer()
            .child(
                div()
                    .text_base()
                    .text_color(rgb(0xFFFFFF))
                    .child("\u{1F50D}"), // search icon 🔍
            )
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(rgb(0xFFFFFF))
                    .child("SEARCH"),
            )
    }

    /// Render the producer client list section
    fn render_producer_list(&self, cx: &mut Context<Self>) -> Div {
        div()
            .flex()
            .flex_col()
            .gap_6()
            .w_full()
            .flex_1()
            .child(self.render_producer_list_header(self.producer_clients.len()))
            .child(self.render_producer_grid(cx))
    }

    /// Render the producer list header
    fn render_producer_list_header(&self, count: usize) -> Div {
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
                    .child("Producer Clients"),
            )
            .child(
                div()
                    .px_3()
                    .py_1()
                    .rounded(px(12.0))
                    .bg(rgb(0xF5F5F7))
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .text_color(rgb(0x86868B))
                            .child(count.to_string()),
                    ),
            )
    }

    /// Render the producer client cards grid
    fn render_producer_grid(&self, cx: &mut Context<Self>) -> Div {
        // Create grid rows with 2 cards each
        let rows: Vec<_> = self.producer_clients.chunks(2).collect();

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
                    .children(row.iter().map(|client| self.render_producer_card(client, cx)))
            }))
    }

    /// Render a single producer client card
    fn render_producer_card(&self, client: &ProducerClientData, cx: &mut Context<Self>) -> Div {
        div()
            .flex_1()
            .min_w(px(400.0))
            .rounded(px(16.0))
            .bg(rgb(0xFFFFFF))
            .border_1()
            .border_color(rgb(0xE5E5E7))
            .p_6()
            .flex()
            .flex_col()
            .gap_5()
            .child(self.render_card_header(client))
            .child(self.render_card_stats(client))
            .child(div().h(px(1.0)).bg(rgb(0xE5E5E7)))
            .child(self.render_card_actions(client, cx))
    }

    /// Render the card header
    fn render_card_header(&self, client: &ProducerClientData) -> Div {
        let lang_color = client.icon_color;
        let lang_bg = client.icon_bg;

        div()
            .flex()
            .items_center()
            .gap_4()
            .child(
                div()
                    .w(px(64.0))
                    .h(px(64.0))
                    .flex()
                    .items_center()
                    .justify_center()
                    .rounded(px(16.0))
                    .bg(client.icon_bg)
                    .child(
                        div()
                            .text_2xl()
                            .text_color(client.icon_color)
                            .child("\u{1F4E4}"), // send icon emoji ✈️
                    ),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap_2()
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
                                    .child(client.client_id.clone()),
                            )
                            .child(self.render_status_badge(client.status)),
                    )
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_3()
                            .child(self.render_language_badge(&client.language, lang_color, lang_bg))
                            .child(
                                div()
                                    .text_sm()
                                    .text_color(rgb(0x86868B))
                                    .child(client.version.clone()),
                            ),
                    ),
            )
    }

    /// Render status badge
    fn render_status_badge(&self, status: ProducerStatus) -> Div {
        div()
            .px_3()
            .py_1()
            .rounded(px(12.0))
            .bg(status.bg_color())
            .child(
                div()
                    .text_xs()
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(status.color())
                    .child(status.display_name()),
            )
    }

    /// Render language badge
    fn render_language_badge(&self, language: &str, color: Rgba, bg: Rgba) -> Div {
        div()
            .px_2()
            .py_1()
            .rounded(px(6.0))
            .bg(bg)
            .child(
                div()
                    .text_xs()
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(color)
                    .child(language.to_string()),
            )
    }

    /// Render the card statistics
    fn render_card_stats(&self, client: &ProducerClientData) -> Div {
        div()
            .flex()
            .items_center()
            .gap(px(40.0))
            .child(self.render_stat("Address", &client.address))
            .child(self.render_stat("Connection ID", &client.connection_id))
            .child(self.render_stat("Language", &client.language))
            .child(self.render_stat("Version", &client.version))
    }

    /// Render a single stat
    fn render_stat(&self, label: &'static str, value: &str) -> Div {
        div()
            .flex()
            .flex_col()
            .gap_1()
            .child(
                div()
                    .text_xs()
                    .text_color(rgb(0x86868B))
                    .child(label.to_string()),
            )
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(rgb(0x1D1D1F))
                    .child(value.to_string()),
            )
    }

    /// Render the card action buttons
    fn render_card_actions(
        &self,
        client: &ProducerClientData,
        cx: &mut Context<Self>,
    ) -> Div {
        let producer_index = self
            .producer_clients
            .iter()
            .position(|c| c.client_id == client.client_id)
            .unwrap();

        div()
            .flex()
            .items_center()
            .gap(px(10.0))
            .w_full()
            .child(self.render_detail_button(producer_index, cx))
            .child(self.render_action_button("\u{1F504}", "Refresh", rgb(0x1D1D1F), rgb(0xF5F5F7)))
            .child(self.render_action_button("\u{1F517}", "Disconnect", rgb(0xFF3B30), rgb(0xFFE5E5)))
    }

    /// Render a detail button with click handler
    fn render_detail_button(&self, producer_index: usize, cx: &mut Context<Self>) -> impl IntoElement {
        let is_selected = self.modal_producer_index == Some(producer_index);
        let button_id: SharedString = format!("detail-btn-{}", producer_index).into();

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
            .border_color(if is_selected {
                rgb(0x007AFF)
            } else {
                rgb(0xE5E5E7)
            })
            .bg(if is_selected { rgb(0x007AFF) } else { rgb(0xF5F5F7) })
            .cursor_pointer()
            .on_click(cx.listener(move |this, _event, _window, cx| {
                if this.modal_producer_index == Some(producer_index) {
                    println!("Closing modal for producer {}", producer_index);
                    this.modal_producer_index = None;
                } else {
                    println!("Opening modal for producer {}", producer_index);
                    this.modal_producer_index = Some(producer_index);
                }
                cx.notify();
            }))
            .child(
                div()
                    .text_sm()
                    .text_color(if is_selected {
                        rgb(0xFFFFFF)
                    } else {
                        rgb(0x1D1D1F)
                    })
                    .child("\u{1F441}"), // visibility icon 👁
            )
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(if is_selected {
                        rgb(0xFFFFFF)
                    } else {
                        rgb(0x1D1D1F)
                    })
                    .child("Detail"),
            )
    }

    /// Render an action button
    fn render_action_button(
        &self,
        icon: &'static str,
        text: &'static str,
        text_color: Rgba,
        bg: Rgba,
    ) -> Div {
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
        if let Some(index) = self.modal_producer_index {
            if let Some(client) = self.producer_clients.get(index) {
                return div()
                    .absolute()
                    .inset_0()
                    .flex()
                    .items_center()
                    .justify_center()
                    .bg(rgba(0x000000AA))
                    .child(self.render_modal_content(client, cx));
            }
        }
        div()
    }

    /// Render the modal content for a specific producer client
    fn render_modal_content(&self, client: &ProducerClientData, cx: &mut Context<Self>) -> Div {
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
            .child(self.render_modal_header(client, cx))
            .child(self.render_modal_body(client))
    }

    /// Render the modal header
    fn render_modal_header(
        &self,
        client: &ProducerClientData,
        cx: &mut Context<Self>,
    ) -> Div {
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
                            .bg(client.icon_bg)
                            .child(
                                div()
                                    .text_2xl()
                                    .text_color(client.icon_color)
                                    .child("\u{1F4E4}"), // send icon emoji ✈️
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
                                    .child(client.client_id.clone()),
                            )
                            .child(
                                div()
                                    .text_sm()
                                    .text_color(rgb(0x86868B))
                                    .child(format!(
                                        "{} / {}",
                                        client.producer_group, client.topic
                                    )),
                            ),
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
                this.modal_producer_index = None;
                cx.notify();
            }))
            .child(div().text_lg().text_color(rgb(0x86868B)).child("\u{2715}"))
    }

    /// Render the modal body
    fn render_modal_body(&self, client: &ProducerClientData) -> Div {
        div()
            .flex()
            .flex_col()
            .gap_5()
            .p_6()
            .flex_1()
            .child(self.render_info_section(client))
            .child(self.render_stats_section(client))
            .child(self.render_metrics_section())
    }

    /// Render the information section
    fn render_info_section(&self, client: &ProducerClientData) -> Div {
        let info = [
            ("Producer Group", client.producer_group.clone()),
            ("Topic", client.topic.clone()),
            ("Address", client.address.clone()),
            ("Connection ID", client.connection_id.clone()),
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
                    .child("Information"),
            )
            .child(
                div()
                    .rounded(px(12.0))
                    .bg(rgb(0xF5F5F7))
                    .p_4()
                    .flex()
                    .flex_col()
                    .gap_3()
                    .children(info.iter().map(|(key, value)| {
                        div()
                            .flex()
                            .items_center()
                            .justify_between()
                            .child(
                                div().text_sm().text_color(rgb(0x86868B)).child((*key).to_string()),
                            )
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

    /// Render the statistics section
    fn render_stats_section(&self, _client: &ProducerClientData) -> Div {
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
                div()
                    .flex()
                    .gap_4()
                    .children(
                        [
                            ("Messages Sent", "2,340", rgb(0x007AFF)),
                            ("Send TPS", "1,245", rgb(0x34C759)),
                            ("Avg Latency", "2.5ms", rgb(0xFF9500)),
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
                                .child(div().text_sm().text_color(rgb(0x86868B)).child((*label).to_string()))
                                .child(
                                    div()
                                        .text_2xl()
                                        .font_weight(FontWeight::BOLD)
                                        .text_color(*color)
                                        .child(value.to_string()),
                                )
                        }),
                    ),
            )
    }

    /// Render the metrics section
    fn render_metrics_section(&self) -> Div {
        div()
            .flex()
            .flex_col()
            .gap_3()
            .child(
                div()
                    .text_base()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(rgb(0x1D1D1F))
                    .child("Recent Metrics"),
            )
            .child(
                div()
                    .rounded(px(12.0))
                    .bg(rgb(0xF5F5F7))
                    .p_4()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .child(
                        div()
                            .text_sm()
                            .text_color(rgb(0x86868B))
                            .child("Last 5 minutes throughput"),
                    )
                    .child(
                        div()
                            .h(px(80.0))
                            .flex()
                            .items_end()
                            .gap_2()
                            .children((0..10).map(|_| {
                                div()
                                    .flex_1()
                                    .bg(rgb(0x007AFF))
                                    .rounded(px(4.0))
                            })),
                    ),
            )
    }
}

impl Render for ProducerView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.render_page(cx)
    }
}

impl Default for ProducerView {
    fn default() -> Self {
        Self::new()
    }
}
