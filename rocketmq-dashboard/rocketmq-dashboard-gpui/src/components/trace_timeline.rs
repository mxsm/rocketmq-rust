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

//! One native presentation of returned Trace records, with no inferred causal graph.

use crate::state::Loadable;
use gpui::{App, InteractiveElement as _, ParentElement as _, Styled as _, div};
use gpui_component::ActiveTheme as _;
use rocketmq_dashboard_common::{MessageTextPreview, MessageTrace};

pub(crate) fn render(state: &Loadable<MessageTrace>, cx: &App) -> gpui::Div {
    let body = div().w_full().min_w_0().flex().flex_col().gap_3().text_sm();
    let trace = match state {
        Loadable::Failed { error, .. } => {
            return body.child(text(error.summary().to_owned()).debug_selector(|| "trace-error".into()));
        }
        Loadable::InitialLoading | Loadable::Refreshing(_) => {
            return body.child(text("Loading Trace records…".into()).debug_selector(|| "trace-loading".into()));
        }
        Loadable::Idle | Loadable::Empty => return body.child("Select a candidate message to load its Trace."),
        Loadable::Ready(trace) => trace,
    };
    let mut body = body
        .child(text(format!(
            "Topic: {} · Message ID: {}",
            trace.identity.topic, trace.identity.message_id
        )))
        .child(text(format!("Trace Topic: {}", trace.trace_topic)));
    let Some(producer_id) = &trace.producer_message_id else {
        return body.child(
            text(
                "The verified message has no producer ID. A physical offset alone cannot identify its Trace records."
                    .into(),
            )
            .debug_selector(|| "trace-no-producer-id".into()),
        );
    };
    body = body
        .child(text(format!("Producer ID: {}", producer_id.display())))
        .child(text("Client-reported records indexed by this producer ID. Reused IDs can cover several sends; no edges or end-to-end latency are inferred.".into()));
    if !trace.trace_topic_found {
        return body.child(
            text("No route was returned for this Trace Topic.".into()).debug_selector(|| "trace-topic-absent".into()),
        );
    }
    if trace.limit_reached {
        body = body.child(
            text("Trace preview limit reached. Additional records may exist.".into())
                .debug_selector(|| "trace-truncated".into()),
        );
    }
    if !trace.unanswered_brokers.is_empty() {
        body = body.child(
            text(format!(
                "Partial read; unanswered Brokers: {}",
                trace
                    .unanswered_brokers
                    .iter()
                    .map(MessageTextPreview::display)
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
            .debug_selector(|| "trace-partial".into()),
        );
    }
    if trace.nodes.is_empty() {
        return body.child(
            text("No Trace records returned for this producer ID.".into()).debug_selector(|| "trace-empty".into()),
        );
    }
    body.children(trace.nodes.iter().enumerate().map(|(index, node)| {
        let status = match node.success {
            Some(true) => "Succeeded",
            Some(false) => "Failed",
            None => "Unknown",
        };
        let mut card = div()
            .id(("trace-node", index))
            .debug_selector(move || format!("trace-node-{index}"))
            .w_full()
            .min_w_0()
            .p_3()
            .border_1()
            .border_color(cx.theme().border)
            .rounded_md()
            .flex()
            .flex_col()
            .gap_1()
            .child(text(format!("{} · {}", node.step.label(), label(&node.group))))
            .child(text(format!(
                "Timestamp (UTC): {} · Status: {status}",
                node.timestamp.map_or_else(|| "Unknown".into(), format_time)
            )))
            .child(text(format!(
                "Reported duration: {}",
                node.cost_time_ms
                    .map_or_else(|| "Unknown".into(), |value| format!("{value} ms"))
            )));
        for (name, value) in [
            ("Client host", &node.client_host),
            ("Store host", &node.store_host),
            ("Offset Message ID", &node.offset_message_id),
            ("Transaction state", &node.transaction_state),
        ] {
            if let Some(value) = value {
                card = card.child(text(format!("{name}: {}", value.display())));
            }
        }
        if let Some(retries) = node.retry_times {
            card = card.child(text(format!("Reported retry count: {retries}")));
        }
        card
    }))
}

fn label(value: &Option<MessageTextPreview>) -> String {
    value
        .as_ref()
        .map_or_else(|| "Unknown group".into(), MessageTextPreview::display)
}
fn text(value: String) -> gpui::Div {
    div()
        .w_full()
        .min_w_0()
        .overflow_hidden()
        .whitespace_normal()
        .child(value)
}

pub(crate) fn format_time(value: i64) -> String {
    chrono::DateTime::from_timestamp_millis(value)
        .map_or_else(|| "Unknown".into(), |time| time.format("%Y-%m-%d %H:%M:%S").to_string())
}
