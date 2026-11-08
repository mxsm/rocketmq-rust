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

//! Frozen Message identity and explicit Consumer execution target.

use crate::state::{UiError, UiErrorCode};
use gpui::{AppContext as _, Context, Entity, IntoElement, ParentElement as _, Render, Styled as _, Window, div};
use gpui_component::{
    ActiveTheme as _,
    input::{Input, InputState},
};
use rocketmq_dashboard_common::{ConnectionScope, MessageConsumeCommand, MessageConsumeReceipt, MessageIdentity};

pub(super) struct MessageConsumeForm {
    identity: MessageIdentity,
    scope: ConnectionScope,
    connection_label: String,
    pub group: Entity<InputState>,
    pub client: Entity<InputState>,
    pub submitted: bool,
    pub status: Option<String>,
}
impl MessageConsumeForm {
    pub fn new(
        identity: MessageIdentity,
        scope: ConnectionScope,
        connection_label: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        Self {
            identity,
            scope,
            connection_label,
            group: cx.new(|cx| InputState::new(window, cx).placeholder("Consumer group (required)")),
            client: cx.new(|cx| InputState::new(window, cx).placeholder("Client ID (optional)")),
            submitted: false,
            status: None,
        }
    }
    pub fn command(&self, cx: &gpui::App) -> Result<MessageConsumeCommand, UiError> {
        let client = self.client.read(cx).value().trim().to_string();
        let command = MessageConsumeCommand {
            identity: self.identity.clone(),
            scope: self.scope,
            consumer_group: self.group.read(cx).value().trim().to_string(),
            client_id: (!client.is_empty()).then_some(client),
        };
        command
            .validate()
            .map_err(|error| UiError::new(error.summary(), UiErrorCode::Validation, false))?;
        Ok(command)
    }
    pub fn clear(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.group.update(cx, |input, cx| input.set_value("", window, cx));
        self.client.update(cx, |input, cx| input.set_value("", window, cx));
    }
}
pub(super) fn receipt_text(receipt: MessageConsumeReceipt) -> String {
    format!(
        "{} · consumeResult: {} · success: {}. {}",
        if receipt.is_success() {
            "Direct consume succeeded"
        } else {
            "Direct consume did not confirm success"
        },
        receipt.code.label(),
        receipt.is_success(),
        receipt.remark()
    )
}
impl Render for MessageConsumeForm {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div().flex().flex_col().gap_3()
            .child(format!("Topic: {}", self.identity.topic))
            .child(format!("Message ID: {}", self.identity.message_id))
            .child(self.connection_label.clone())
            .child(format!("Execution scope: {}", if self.scope == ConnectionScope::NameServer { "NameServer Direct; the SDK resolves the Consumer target" } else { "Proxy (unavailable)" }))
            .child("Review the Consumer group and optional exact Client ID before confirming. This triggers one direct-consume attempt.")
            .child(Input::new(&self.group).disabled(self.submitted))
            .child(Input::new(&self.client).disabled(self.submitted))
            .children(self.status.as_ref().map(|status| div().text_sm().text_color(cx.theme().muted_foreground).child(status.clone())))
    }
}
