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

//! Frozen body-free recovery confirmation; no form supplies execution attribution.

use gpui::{Context, IntoElement, ParentElement as _, Render, Styled as _, Window, div, px};
use gpui_component::scroll::ScrollableElement as _;
use rocketmq_dashboard_common::DlqResendTarget;
pub(super) struct DlqConfirmation {
    pub targets: Vec<DlqResendTarget>,
    pub submitted: bool,
    pub connection: String,
}
impl Render for DlqConfirmation {
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        div().flex().flex_col().gap_3()
            .child(self.connection.clone())
            .child("NameServer Direct · Each original message is sent once to the Group through its verified Broker. Client selection uses a registered Group client.")
            .child(format!("Review {} unique original Topic / Group / Message ID tuples:", self.targets.len()))
            .child(div().h(px(200.)).overflow_y_scrollbar().child(div().w_full().flex().flex_col().gap_2().children(self.targets.iter().map(|target| div().text_sm().whitespace_normal()
                .child(format!("Topic: {} · Group: {}", target.original().topic, target.consumer_group()))
                .child(div().child(format!("Original ID: {}", target.original().message_id)))
                .child(div().child(format!("DLQ source ID: {}", target.source().message_id)))))))
            .child("Partial outcomes are retained. Successes and unconfirmed entries are excluded from failed-entry retry.")
            .children(self.submitted.then(|| div().child("Batch admitted. Closing this dialog does not cancel execution.")))
    }
}
