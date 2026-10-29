// Copyright 2026 The RocketMQ Rust Authors
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

//! Four-tab Message inspector with one lazy, bounded sensitive payload.

use crate::{
    features::dashboard_store::ResourceSlot,
    services::{AppServices, messages::MessageRequestScope},
    state::Loadable,
};
use gpui::{
    Context, EventEmitter, InteractiveElement as _, IntoElement, ParentElement as _, Render, Styled as _, Task, Window,
    div,
};
use gpui_component::{
    ActiveTheme as _,
    button::Button,
    scroll::ScrollableElement as _,
    tab::{Tab, TabBar},
};
use rocketmq_dashboard_common::{MESSAGE_BODY_MAX_BYTES, MessageMetadataRow, MessagePayload};

pub enum MessageDetailIntent {
    Consume,
}
#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum MessageTab {
    #[default]
    Overview,
    Properties,
    Body,
    Trace,
}

pub struct MessageDetail {
    services: AppServices,
    revision: u64,
    row: MessageMetadataRow,
    tab: MessageTab,
    payload: ResourceSlot<MessagePayload>,
    task: Option<Task<()>>,
    preview: Option<BodyPreview>,
    json: bool,
    copied: bool,
}
impl EventEmitter<MessageDetailIntent> for MessageDetail {}
impl MessageDetail {
    pub fn new(services: AppServices, revision: u64, row: MessageMetadataRow) -> Self {
        Self {
            services,
            revision,
            row,
            tab: MessageTab::Overview,
            payload: ResourceSlot::default(),
            task: None,
            preview: None,
            json: false,
            copied: false,
        }
    }
    fn load_payload(&mut self, cx: &mut Context<Self>) {
        let Some(request) = self.payload.begin(self.revision) else {
            return;
        };
        let services = self.services.clone();
        let identity = self.row.identity.clone();
        self.task = Some(cx.spawn(async move |this, cx| {
            let scope = MessageRequestScope {
                revision: request.revision(),
                epoch: request.epoch(),
            };
            let result = services.message_payload(scope, identity).await.map(Some);
            let _ = this.update(cx, |detail, cx| {
                if !detail.payload.finish(request, detail.revision, result) {
                    return;
                }
                detail.preview = detail.payload.state.value().map(BodyPreview::from_payload);
                detail.json = detail.preview.as_ref().is_some_and(|preview| preview.pretty.is_some());
                cx.notify();
            });
        }));
    }
    pub fn clear_sensitive(&mut self, cx: &mut Context<Self>) {
        self.task = None;
        self.payload.clear();
        self.preview = None;
        self.json = false;
        cx.notify();
    }
    fn select_tab(&mut self, tab: MessageTab, cx: &mut Context<Self>) {
        self.tab = tab;
        if matches!(tab, MessageTab::Body | MessageTab::Properties) && matches!(self.payload.state, Loadable::Idle) {
            self.load_payload(cx);
        }
        cx.notify();
    }
    #[cfg(test)]
    pub(crate) fn has_payload(&self) -> bool {
        self.payload.state.value().is_some() || self.preview.is_some()
    }

    fn content(&self, cx: &Context<Self>) -> gpui::Div {
        match self.tab {
            MessageTab::Overview => div()
                .flex()
                .flex_col()
                .gap_2()
                .child(format!("Topic: {}", self.row.identity.topic))
                .child(format!("Message ID: {}", self.row.identity.message_id))
                .child(
                    Button::new("message-copy-id")
                        .label("Copy Message ID")
                        .outline()
                        .debug_selector(|| "message-copy-id".into())
                        .on_click(cx.listener(|detail, _, _, cx| {
                            cx.write_to_clipboard(gpui::ClipboardItem::new_string(
                                detail.row.identity.message_id.clone(),
                            ));
                            detail.copied = true;
                            cx.notify();
                        })),
                )
                .children(self.copied.then(|| div().child("Message ID copied.")))
                .child(format!(
                    "Tags: {}",
                    self.row
                        .tags
                        .as_ref()
                        .map_or_else(|| "-".into(), |value| value.display())
                ))
                .child(format!(
                    "Keys: {}",
                    self.row
                        .keys
                        .as_ref()
                        .map_or_else(|| "-".into(), |value| value.display())
                ))
                .child(format!("Born host: {}", self.row.born_host.display()))
                .child(format!("Store host: {}", self.row.store_host.display()))
                .child(format!(
                    "Born (UTC): {} · Stored (UTC): {}",
                    format_time(self.row.born_timestamp),
                    format_time(self.row.store_timestamp)
                ))
                .child(format!(
                    "Queue: {} · Queue offset: {}",
                    self.row.queue_id, self.row.queue_offset
                ))
                .child(format!(
                    "Stored bytes: {} · Reconsumed: {}",
                    self.row.store_size, self.row.reconsume_times
                ))
                .child(format!(
                    "Flags: {} · System flags: {} · Transaction offset: {}",
                    self.row.flag, self.row.sys_flag, self.row.prepared_transaction_offset
                ))
                .child(
                    Button::new("message-consume")
                        .label("Consume directly…")
                        .outline()
                        .debug_selector(|| "message-consume".into())
                        .on_click(cx.listener(|_, _, _, cx| cx.emit(MessageDetailIntent::Consume))),
                ),
            MessageTab::Trace => div().child("Message Trace lookup is currently unavailable."),
            MessageTab::Body | MessageTab::Properties => {
                let Some(payload) = self.payload.state.value() else {
                    return match &self.payload.state {
                        Loadable::Failed { error, .. } => div()
                            .flex()
                            .flex_col()
                            .gap_2()
                            .child(error.summary().to_string())
                            .child(
                                Button::new("message-detail-retry")
                                    .label("Retry details")
                                    .outline()
                                    .on_click(cx.listener(|detail, _, _, cx| detail.load_payload(cx))),
                            ),
                        _ => div().child("Loading Message details…"),
                    };
                };
                if self.tab == MessageTab::Properties {
                    return div()
                        .flex()
                        .flex_col()
                        .gap_2()
                        .child(if payload.properties_truncated {
                            "Properties preview is limited to 128 entries / 64 KiB."
                        } else {
                            "Message properties"
                        })
                        .children(
                            payload
                                .properties()
                                .iter()
                                .map(|(key, value)| div().flex().gap_2().child(key.clone()).child(value.clone())),
                        )
                        .children(
                            payload
                                .properties()
                                .is_empty()
                                .then(|| div().child("No properties returned.")),
                        );
                }
                let Some(preview) = &self.preview else {
                    return div().child("Body preview unavailable.");
                };
                let text = if self.json {
                    preview.pretty.as_ref().unwrap_or(&preview.raw)
                } else {
                    &preview.raw
                };
                div()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .child(format!(
                        "{} bytes in source · {}",
                        payload.original_body_bytes,
                        if preview.utf8 {
                            "UTF-8 text"
                        } else {
                            "Binary / lossy UTF-8 preview"
                        }
                    ))
                    .children((payload.body_truncated || preview.limited).then(|| {
                        div().child("Preview truncated to a bounded size; the original message is unchanged.")
                    }))
                    .children(preview.pretty.as_ref().map(|_| {
                        Button::new("message-body-format")
                            .label(if self.json { "Show plain text" } else { "Format JSON" })
                            .outline()
                            .debug_selector(|| "message-body-format".into())
                            .on_click(cx.listener(|detail, _, _, cx| {
                                detail.json = !detail.json;
                                cx.notify();
                            }))
                    }))
                    .child(
                        div()
                            .id("message-body-preview")
                            .debug_selector(|| "message-body-preview".into())
                            .text_sm()
                            .font_family("monospace")
                            .whitespace_normal()
                            .child(text.clone()),
                    )
            }
        }
    }
}
impl Render for MessageDetail {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let index = match self.tab {
            MessageTab::Overview => 0,
            MessageTab::Properties => 1,
            MessageTab::Body => 2,
            MessageTab::Trace => 3,
        };
        div()
            .size_full()
            .flex()
            .flex_col()
            .gap_3()
            .text_color(cx.theme().foreground)
            .child(
                TabBar::new("message-detail-tabs")
                    .selected_index(index)
                    .children(
                        [
                            ("Overview", "message-tab-overview"),
                            ("Properties", "message-tab-properties"),
                            ("Body", "message-tab-body"),
                            ("Trace", "message-tab-trace"),
                        ]
                        .into_iter()
                        .map(|(label, id)| Tab::new().label(label).debug_selector(move || id.into())),
                    )
                    .on_click(cx.listener(|detail, index, _, cx| {
                        detail.select_tab(
                            match *index {
                                0 => MessageTab::Overview,
                                1 => MessageTab::Properties,
                                2 => MessageTab::Body,
                                _ => MessageTab::Trace,
                            },
                            cx,
                        )
                    })),
            )
            .child(div().flex_1().min_h_0().overflow_y_scrollbar().child(self.content(cx)))
    }
}

pub(super) fn format_time(value: i64) -> String {
    chrono::DateTime::from_timestamp_millis(value)
        .map_or_else(|| "Unknown".into(), |time| time.format("%Y-%m-%d %H:%M:%S").to_string())
}

struct BodyPreview {
    raw: String,
    pretty: Option<String>,
    utf8: bool,
    limited: bool,
}
impl BodyPreview {
    fn from_payload(payload: &MessagePayload) -> Self {
        let utf8 = std::str::from_utf8(payload.body()).is_ok();
        let mut raw = String::from_utf8_lossy(payload.body()).into_owned();
        let mut limited = raw.len() > MESSAGE_BODY_MAX_BYTES;
        raw.truncate(raw.floor_char_boundary(MESSAGE_BODY_MAX_BYTES));
        let pretty = if utf8 && !payload.body_truncated {
            serde_json::from_slice::<serde_json::Value>(payload.body())
                .ok()
                .and_then(|value| serde_json::to_string_pretty(&value).ok())
                .map(|mut text| {
                    if text.len() > MESSAGE_BODY_MAX_BYTES {
                        limited = true;
                        text.truncate(text.floor_char_boundary(MESSAGE_BODY_MAX_BYTES));
                    }
                    text
                })
        } else {
            None
        };
        Self {
            raw,
            pretty,
            utf8,
            limited,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::messages::test_support::row;
    use std::collections::BTreeMap;
    #[test]
    fn message_body_preview_formats_json_preserves_text_and_bounds_lossy_or_expanded_output() {
        let json = MessagePayload::bounded(row().identity, br#"{"key":[1,2]}"#.to_vec(), BTreeMap::new());
        let preview = BodyPreview::from_payload(&json);
        assert!(preview.pretty.unwrap().contains("\n"));
        assert_eq!(preview.raw, r#"{"key":[1,2]}"#);
        let text = MessagePayload::bounded(row().identity, b"plain\ntext".to_vec(), BTreeMap::new());
        let preview = BodyPreview::from_payload(&text);
        assert!(preview.pretty.is_none());
        assert_eq!(preview.raw, "plain\ntext");
        let binary = MessagePayload::bounded(row().identity, vec![0xff; MESSAGE_BODY_MAX_BYTES + 1], BTreeMap::new());
        let preview = BodyPreview::from_payload(&binary);
        assert!(!preview.utf8);
        assert!(preview.limited);
        assert!(preview.raw.len() <= MESSAGE_BODY_MAX_BYTES);
        assert!(preview.pretty.is_none());
    }
}
