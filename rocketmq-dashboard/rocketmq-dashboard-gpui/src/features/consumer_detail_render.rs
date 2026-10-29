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

//! Typed Consumer clients, queue progress and per-Broker configuration presentation.
use super::{ConsumerDetail, loading_or_empty, observation_state_label};
use gpui::prelude::FluentBuilder as _;
use gpui::{ClipboardItem, Context, InteractiveElement as _, ParentElement as _, Styled as _, Window, div};
use gpui_component::{
    ActiveTheme as _, Disableable as _, Sizable as _, button::Button, scroll::ScrollableElement as _,
};
use rocketmq_dashboard_common::ConsumerDiagnosticKind;
impl ConsumerDetail {
    pub(super) fn render_clients(&self, cx: &mut Context<Self>) -> gpui::Div {
        let mut body = div().w_full().min_w_0().flex().flex_col().gap_3();
        if let Some(observation) = self.store.clients.state.value() {
            body = body.child(observation_state_label(observation));
            if let Some(clients) = observation.value() {
                for (index, client) in clients.clients.iter().enumerate() {
                    let running_client = client.identity.clone();
                    let jstack_client = client.identity.clone();
                    let copy_client = client.identity.as_str().to_owned();
                    let copy_address = client.address.clone();
                    body = body.child(
                        div()
                            .w_full()
                            .min_w_0()
                            .p_3()
                            .rounded_md()
                            .border_1()
                            .border_color(cx.theme().border)
                            .flex()
                            .flex_col()
                            .gap_2()
                            .child(
                                detail_text(format!("Client ID: {}", client.identity.as_str()))
                                    .debug_selector(move || format!("consumer-client-identity-{index}")),
                            )
                            .child(detail_text(format!("Address: {}", client.address)))
                            .child(detail_text(format!(
                                "{} · {} (v{})",
                                client.language, client.version_description, client.version
                            )))
                            .child(
                                div()
                                    .w_full()
                                    .min_w_0()
                                    .flex()
                                    .flex_wrap()
                                    .gap_2()
                                    .child(
                                        Button::new(("copy-consumer-client", index))
                                            .label("Copy client ID")
                                            .small()
                                            .outline()
                                            .debug_selector(move || format!("consumer-copy-client-{index}"))
                                            .on_click(move |_, _, cx| {
                                                cx.write_to_clipboard(ClipboardItem::new_string(copy_client.clone()))
                                            }),
                                    )
                                    .child(
                                        Button::new(("copy-consumer-address", index))
                                            .label("Copy address")
                                            .small()
                                            .outline()
                                            .on_click(move |_, _, cx| {
                                                cx.write_to_clipboard(ClipboardItem::new_string(copy_address.clone()))
                                            }),
                                    ),
                            )
                            .child(
                                div()
                                    .w_full()
                                    .min_w_0()
                                    .flex()
                                    .flex_wrap()
                                    .gap_2()
                                    .child(
                                        Button::new(("running-info", index))
                                            .debug_selector(move || format!("consumer-running-info-{index}"))
                                            .label("Running Info · Direct")
                                            .disabled(
                                                self.capabilities.diagnostics
                                                    != rocketmq_dashboard_common::CapabilityAvailability::Available,
                                            )
                                            .small()
                                            .outline()
                                            .on_click(cx.listener(move |detail, _, _, cx| {
                                                detail.load_diagnostic(
                                                    running_client.clone(),
                                                    ConsumerDiagnosticKind::RunningInfo,
                                                    cx,
                                                );
                                            })),
                                    )
                                    .child(
                                        Button::new(("jstack", index))
                                            .debug_selector(move || format!("consumer-jstack-{index}"))
                                            .label("JStack · Direct")
                                            .disabled(
                                                self.capabilities.diagnostics
                                                    != rocketmq_dashboard_common::CapabilityAvailability::Available,
                                            )
                                            .small()
                                            .outline()
                                            .on_click(cx.listener(move |detail, _, _, cx| {
                                                detail.load_diagnostic(
                                                    jstack_client.clone(),
                                                    ConsumerDiagnosticKind::Jstack,
                                                    cx,
                                                );
                                            })),
                                    ),
                            ),
                    );
                }
                body = body.child("Subscriptions");
                for subscription in &clients.subscriptions {
                    body = body.child(detail_text(format!(
                        "{} · {} · {}",
                        subscription.topic, subscription.expression_type, subscription.expression
                    )));
                }
            }
        } else {
            body = body.child("Loading clients…");
        }
        if let Some(payload) = self.store.diagnostic.state.value() {
            body = body
                .child(div().w_full().min_w_0().child("Running Info subscriptions").children(
                    payload.subscriptions().iter().map(|subscription| {
                        detail_text(format!(
                            "{} · {} · {}",
                            subscription.topic, subscription.expression_type, subscription.expression
                        ))
                    }),
                ))
                .child(div().w_full().min_w_0().child("Process queues (bounded)").children(
                    payload.process_queues().iter().map(|queue| {
                        detail_text(format!(
                            "{} / {} / q{} · cached={} ({} MiB) · commit={} · dropped={} · last consume={} ms",
                            queue.topic,
                            queue.broker_name,
                            queue.queue_id,
                            queue.cached_message_count,
                            queue.cached_message_size_in_mib,
                            queue.commit_offset,
                            queue.dropped,
                            queue.last_consume_timestamp
                        ))
                    }),
                ));
            body = body.child(
                div()
                    .id("consumer-diagnostic")
                    .debug_selector(|| "consumer-diagnostic".to_owned())
                    .w_full()
                    .min_w_0()
                    .mt_3()
                    .p_3()
                    .rounded_md()
                    .bg(cx.theme().muted)
                    .flex()
                    .flex_col()
                    .gap_2()
                    .child(if payload.truncated() {
                        "Diagnostic output (truncated)"
                    } else {
                        "Diagnostic output"
                    })
                    .child(
                        div()
                            .id("consumer-diagnostic-scroll")
                            .debug_selector(|| "consumer-diagnostic-scroll".to_owned())
                            .w_full()
                            .min_w_0()
                            .h_64()
                            .overflow_hidden()
                            .child(
                                div().size_full().overflow_scrollbar().child(
                                    div()
                                        .id("consumer-diagnostic-text")
                                        .debug_selector(|| "consumer-diagnostic-text".to_owned())
                                        .flex_shrink_0()
                                        .flex()
                                        .flex_col()
                                        .items_start()
                                        .whitespace_nowrap()
                                        .text_sm()
                                        .children(
                                            payload
                                                .properties()
                                                .iter()
                                                .map(|(key, value)| div().child(format!("{key}: {value}"))),
                                        )
                                        .when_some(payload.text(), |this, text| {
                                            this.child(div().child(text.to_owned()))
                                        }),
                                ),
                            ),
                    ),
            );
        }
        body
    }

    pub(super) fn render_progress(&self, actions: bool, cx: &mut Context<Self>) -> gpui::Div {
        let observation = if actions {
            self.store.offset_actions.state.value()
        } else {
            self.store.progress.state.value()
        };
        match observation {
            Some(observation) => {
                let mut body = div()
                    .w_full()
                    .min_w_0()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .child(observation_state_label(observation));
                if let Some(progress) = observation.value() {
                    body = body.child(format!("Signed total lag: {}. Negative lag means the consumer offset is ahead of the observed Broker offset.", progress.total_delta));
                    for (index, row) in progress.rows.iter().enumerate() {
                        let mut item = div().w_full().min_w_0().flex().flex_col().gap_1().child(format!(
                            "{} / {} / queue {} · broker={} consumer={} delta={} · last queue timestamp={} ms",
                            row.topic,
                            row.broker_name,
                            row.queue_id,
                            row.broker_offset,
                            row.consumer_offset,
                            row.delta,
                            row.last_timestamp
                        ));
                        if actions
                            && self.capabilities.offset_actions
                                == rocketmq_dashboard_common::CapabilityAvailability::Available
                            && let Some(cluster_name) = self.exact_cluster_for_broker(&row.broker_name)
                        {
                            let reset_topic = row.topic.clone();
                            let reset_cluster = cluster_name.clone();
                            let skip_topic = row.topic.clone();
                            item = item.child(
                                div()
                                    .flex()
                                    .flex_wrap()
                                    .items_center()
                                    .gap_2()
                                    .child(
                                        Button::new(("consumer-reset-offset", index))
                                            .debug_selector(move || format!("consumer-reset-offset-{index}"))
                                            .label("Reset offset")
                                            .small()
                                            .outline()
                                            .on_click(cx.listener(move |detail, _, window, cx| {
                                                detail.request_offset_action(
                                                    reset_topic.clone(),
                                                    reset_cluster.clone(),
                                                    false,
                                                    window,
                                                    cx,
                                                );
                                            })),
                                    )
                                    .child(
                                        Button::new(("consumer-skip-offset", index))
                                            .debug_selector(move || format!("consumer-skip-offset-{index}"))
                                            .label("Skip accumulated")
                                            .small()
                                            .outline()
                                            .on_click(cx.listener(move |detail, _, window, cx| {
                                                detail.request_offset_action(
                                                    skip_topic.clone(),
                                                    cluster_name.clone(),
                                                    true,
                                                    window,
                                                    cx,
                                                );
                                            })),
                                    ),
                            );
                        }
                        body = body.child(item);
                    }
                }
                if actions {
                    body = body.child(
                        "Each action reuses the D4 exact Topic/group/one-cluster coordinator; detailed broker/queue outcomes remain partial and are never broadly replayed.",
                    );
                }
                body
            }
            None => div().child("Loading progress…"),
        }
    }

    pub(super) fn render_configuration(&self, _window: &mut Window, cx: &mut Context<Self>) -> gpui::Div {
        let Some(configuration) = self.store.configuration.state.value() else {
            return loading_or_empty(&self.store.configuration.state);
        };
        let differences = configuration.differing_fields();
        let mut body = div()
            .w_full()
            .min_w_0()
            .flex()
            .flex_col()
            .gap_3()
            .child(format!(
                "Broker Direct configuration · {:?} · {} observed target(s)",
                configuration.observation,
                configuration.snapshots.len()
            ))
            .child(if differences.is_empty() {
                "No differences among observed targets. Missing or failed targets are not assumed equal.".to_owned()
            } else {
                format!("Cross-Broker differences: {}", differences.join(", "))
            });
        for failure in &configuration.failures {
            body = body.child(div().child(format!("{} · {:?} · {:?}", failure.target, failure.stage, failure.code)));
        }
        for (index, snapshot) in configuration.snapshots.iter().enumerate() {
            let editable = self.capabilities.edit == rocketmq_dashboard_common::CapabilityAvailability::Available;
            let selected = snapshot.clone();
            let values = snapshot.entries;
            let read_only = &snapshot.read_only;
            let policy = match &read_only.retry_policy {
                rocketmq_dashboard_common::ConsumerRetryPolicy::DefaultCustomized => {
                    "Default customized retry policy".to_owned()
                }
                rocketmq_dashboard_common::ConsumerRetryPolicy::DefaultFallback => {
                    "Exponential selected without settings; Broker default fallback".to_owned()
                }
                rocketmq_dashboard_common::ConsumerRetryPolicy::Customized { delays_ms } => {
                    format!("Customized retry delays (ms): {delays_ms:?}")
                }
                rocketmq_dashboard_common::ConsumerRetryPolicy::Exponential {
                    initial_ms,
                    maximum_ms,
                    multiplier,
                } => format!(
                    "Exponential retry: initial={initial_ms} ms, maximum={maximum_ms} ms, multiplier={multiplier}"
                ),
            };
            body = body.child(div().w_full().min_w_0().p_3().rounded_md().border_1().border_color(cx.theme().border).flex().flex_col().gap_2()
                .child(format!("{} / {} / {} · version {}",snapshot.identity.target.cluster_name(),snapshot.identity.target.broker_name(),snapshot.identity.target.broker_address(),snapshot.generation))
                .child(format!("Consumption: enabled={} · from minimum={} · broadcast={} · orderly={}",values.consume_enable,values.consume_from_min_enable,values.consume_broadcast_enable,values.consume_message_orderly))
                .child(format!("Retry/routing: max={} · queues={} · timeout={} min · Broker ID={} · slow Broker ID={}",values.retry_max_times,values.retry_queue_nums,values.consume_timeout_minutes,values.broker_id,values.which_broker_when_consume_slowly))
                .child(format!("Read only: notify IDs changed={} · system flag={}",values.notify_consumer_ids_changed_enable,values.group_sys_flag))
                .child(policy).child(format!("Subscribed Topics: {}",if read_only.subscribed_topics.is_empty() {"None reported".to_owned()} else {read_only.subscribed_topics.join(", ")}))
                .children(read_only.attributes.iter().map(|(key,value)| div().child(format!("{key}: {value}"))))
                .when(read_only.truncated,|card| card.child("Read-only details truncated to the display budget."))
                .when(editable,|card| card.child(Button::new(("consumer-edit-config",index)).debug_selector(move || format!("consumer-edit-config-{index}"))
                    .label("Edit this Broker").small().outline().disabled(self.mutation_task.is_some())
                    .on_click(cx.listener(move |detail,_,window,cx| detail.open_config_dialog(selected.clone(),window,cx)))))
                .when(!editable,|card| card.child("Read only in the configured connection scope. Choose NameServer Direct in connection settings to mutate.")));
        }
        body
    }
}

fn detail_text(value: impl Into<String>) -> gpui::Div {
    div().w_full().min_w_0().whitespace_normal().child(value.into())
}
