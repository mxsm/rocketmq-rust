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

//! Canonical Topic detail tab and resource-state rendering.

use gpui::prelude::FluentBuilder as _;
use gpui::{Context, InteractiveElement as _, IntoElement, ParentElement as _, Render, Styled as _, Window, div};
use gpui_component::{
    ActiveTheme as _, Disableable as _,
    button::{Button, ButtonVariants as _},
    scroll::ScrollableElement as _,
    tab::TabBar,
};

use super::*;

impl TopicDetail {
    fn render_overview(&self, cx: &mut Context<Self>) -> gpui::Div {
        let partial = !self.store.selected.inventory_completeness.is_complete();
        let empty_message = if partial {
            "The Topic was not observed in the partial inventory; presence remains unverified."
        } else {
            "The Topic is absent from the authoritative inventory."
        };
        let resource = render_resource(
            &self.store.overview.state,
            TopicTab::Overview,
            empty_message,
            cx,
            |item, cx| self.render_overview_ready(item, cx),
        );
        div()
            .flex()
            .flex_col()
            .gap_2()
            .when(partial, |this| {
                this.child(
                    div()
                        .id("topic-inventory-partial-evidence")
                        .debug_selector(|| "topic-inventory-partial-evidence".to_owned())
                        .child("Inventory verification is partial; mutations remain disabled until a complete reload.")
                        .child(render_completeness(
                            self.store.selected.inventory_completeness,
                            &self.store.selected.inventory_failures,
                            cx,
                        )),
                )
            })
            .child(resource)
    }

    fn render_overview_ready(&self, item: &TopicInventoryItem, cx: &mut Context<Self>) -> gpui::Div {
        let theme = cx.theme();
        let mutable = item.is_mutable() && self.store.selected.inventory_verified();
        let send_topic = item.identity.clone();
        let delete_topic = item.identity.clone();
        let delete_clusters = item.clusters.clone();
        div()
            .flex()
            .flex_col()
            .child(key_value::render(
                "Category",
                category_label(item.category),
                theme.foreground,
                theme.muted_foreground,
                theme.border,
            ))
            .child(
                div()
                    .flex()
                    .gap_2()
                    .child(
                        Button::new("send-topic-message")
                            .label("Send message")
                            .outline()
                            .disabled(!mutable)
                            .debug_selector(|| "topic-overview-send".to_owned())
                            .on_click(cx.listener(move |_, _, _, cx| {
                                cx.emit(TopicDetailIntent::Send(send_topic.clone()));
                            })),
                    )
                    .child(
                        Button::new("delete-topic")
                            .label("Delete Topic")
                            .danger()
                            .disabled(!mutable)
                            .debug_selector(|| "topic-overview-delete".to_owned())
                            .on_click(cx.listener(move |_, _, _, cx| {
                                cx.emit(TopicDetailIntent::DeleteTopic {
                                    topic: delete_topic.clone(),
                                    clusters: delete_clusters.clone(),
                                });
                            })),
                    ),
            )
            .child(key_value::render(
                "Message type",
                message_type_label(item.message_type),
                theme.foreground,
                theme.muted_foreground,
                theme.border,
            ))
            .child(key_value::render(
                "Clusters",
                item.clusters.join(", "),
                theme.foreground,
                theme.muted_foreground,
                theme.border,
            ))
            .child(key_value::render(
                "Brokers",
                item.brokers.join(", "),
                theme.foreground,
                theme.muted_foreground,
                theme.border,
            ))
    }

    fn render_stats(&self, cx: &mut Context<Self>) -> gpui::Div {
        render_resource(
            &self.store.stats.state,
            TopicTab::Stats,
            "No queue offsets were returned.",
            cx,
            |stats, cx| self.render_stats_ready(stats, cx),
        )
    }

    fn render_stats_ready(
        &self,
        stats: &rocketmq_dashboard_common::TopicStatsView,
        cx: &mut Context<Self>,
    ) -> gpui::Div {
        let theme = cx.theme();
        div()
            .flex()
            .flex_col()
            .gap_2()
            .child(key_value::render(
                "Message count",
                stats.total_message_count.to_string(),
                theme.foreground,
                theme.muted_foreground,
                theme.border,
            ))
            .children(stats.offsets.iter().map(|offset| {
                key_value::render(
                    format!("{} · queue {}", offset.broker_name, offset.queue_id),
                    format!(
                        "min {} · max {} · count {} · activity {}",
                        offset.min_offset,
                        offset.max_offset,
                        offset.message_count(),
                        offset.last_update_timestamp
                    ),
                    theme.foreground,
                    theme.muted_foreground,
                    theme.border,
                )
            }))
            .child(render_completeness(stats.completeness, &stats.failures, cx))
    }

    fn render_route(&self, cx: &mut Context<Self>) -> gpui::Div {
        render_resource(
            &self.store.route.state,
            TopicTab::Route,
            "No route targets were returned.",
            cx,
            |route, cx| self.render_route_ready(route, cx),
        )
    }

    fn render_route_ready(
        &self,
        route: &rocketmq_dashboard_common::TopicRouteView,
        cx: &mut Context<Self>,
    ) -> gpui::Div {
        let theme = cx.theme();
        div()
            .flex()
            .flex_col()
            .gap_2()
            .children(route.brokers.iter().map(|broker| {
                key_value::render(
                    format!("{} · {}", broker.cluster_name, broker.broker_name),
                    format!(
                        "{} address(es) · acting master {}",
                        broker.address_count, broker.acting_master
                    ),
                    theme.foreground,
                    theme.muted_foreground,
                    theme.border,
                )
            }))
            .children(route.queues.iter().map(|queue| {
                key_value::render(
                    format!("{} queues", queue.broker_name),
                    format!(
                        "read {} · write {} · permission {}",
                        queue.read_queue_count,
                        queue.write_queue_count,
                        permission_label(queue.permission)
                    ),
                    theme.foreground,
                    theme.muted_foreground,
                    theme.border,
                )
            }))
    }

    fn render_configuration(&self, cx: &mut Context<Self>) -> gpui::Div {
        render_resource(
            &self.store.configuration.state,
            TopicTab::Configuration,
            "No exact Broker configuration targets were returned.",
            cx,
            |config, cx| self.render_configuration_ready(config, cx),
        )
    }

    fn render_configuration_ready(&self, config: &TopicConfigView, cx: &mut Context<Self>) -> gpui::Div {
        div()
            .flex()
            .flex_col()
            .gap_3()
            .when(!config.inconsistent_fields.is_empty(), |this| {
                this.child(format!(
                    "Inconsistent fields: {}",
                    config
                        .inconsistent_fields
                        .iter()
                        .map(config_field_label)
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            })
            .children(
                config
                    .targets
                    .iter()
                    .enumerate()
                    .map(|(index, target)| self.render_config_target(index, target, cx)),
            )
            .child(render_completeness(config.completeness, &config.failures, cx))
    }

    fn render_config_target(&self, index: usize, target: &TopicConfigTargetView, cx: &mut Context<Self>) -> gpui::Div {
        let theme = cx.theme();
        let intent_target = target.target.clone();
        let expected_version = target.version;
        let read_queue_count = target.read_queue_count;
        let write_queue_count = target.write_queue_count;
        let delete_topic = self.store.selected.item.identity.clone();
        let delete_target = target.target.clone();
        let mutable = self.store.selected.item.is_mutable() && self.store.selected.inventory_verified();
        div()
            .p_3()
            .border_1()
            .border_color(theme.border)
            .rounded_md()
            .flex()
            .flex_col()
            .gap_2()
            .child(format!(
                "{} · {}",
                target.target.cluster_name(),
                target.target.broker_name()
            ))
            .child(format!(
                "read {} · write {} · permission {} · {} · {} · version {}",
                read_queue_count,
                write_queue_count,
                permission_label(target.permission),
                if target.ordered { "ordered" } else { "unordered" },
                message_type_label(target.message_type),
                target.version
            ))
            .child(
                div()
                    .flex()
                    .gap_2()
                    .child(
                        Button::new(("edit-topic-config-target", index))
                            .label("Edit queue counts")
                            .outline()
                            .disabled(!mutable)
                            .debug_selector(move || format!("topic-config-edit-{index}"))
                            .on_click(cx.listener(move |_, _, _, cx| {
                                cx.emit(TopicDetailIntent::EditTarget {
                                    target: intent_target.clone(),
                                    expected_version,
                                    read_queue_count,
                                    write_queue_count,
                                });
                            })),
                    )
                    .child(
                        Button::new(("delete-topic-broker", index))
                            .label("Delete from Broker")
                            .danger()
                            .disabled(!mutable)
                            .debug_selector(move || format!("topic-config-delete-broker-{index}"))
                            .on_click(cx.listener(move |_, _, _, cx| {
                                cx.emit(TopicDetailIntent::DeleteBroker {
                                    topic: delete_topic.clone(),
                                    target: delete_target.clone(),
                                });
                            })),
                    ),
            )
    }

    fn render_consumers(&self, cx: &mut Context<Self>) -> gpui::Div {
        render_resource(
            &self.store.consumers.state,
            TopicTab::Consumers,
            "No consumers are currently associated with this Topic.",
            cx,
            |consumers, cx| self.render_consumers_ready(consumers, cx),
        )
    }

    fn render_consumers_ready(
        &self,
        consumers: &rocketmq_dashboard_common::TopicConsumersView,
        cx: &mut Context<Self>,
    ) -> gpui::Div {
        let theme = cx.theme();
        div()
            .flex()
            .flex_col()
            .gap_2()
            .when(consumers.items.is_empty(), |this| {
                this.child("No consumer rows were returned by the successful targets.")
            })
            .children(consumers.items.iter().enumerate().map(|(index, consumer)| {
                let group = consumer.consumer_group.clone();
                let reset_group = consumer.consumer_group.clone();
                let skip_group = consumer.consumer_group.clone();
                let reset_topic = self.store.selected.item.identity.clone();
                let skip_topic = self.store.selected.item.identity.clone();
                let reset_clusters = self.store.selected.item.clusters.clone();
                let skip_clusters = reset_clusters.clone();
                let mutable = self.store.selected.item.is_mutable() && self.store.selected.inventory_verified();
                div()
                    .p_3()
                    .border_1()
                    .border_color(theme.border)
                    .rounded_md()
                    .flex()
                    .justify_between()
                    .items_center()
                    .child(format!(
                        "{} · lag {} · inflight {}",
                        consumer.consumer_group, consumer.total_diff, consumer.inflight_diff
                    ))
                    .child(
                        div()
                            .flex()
                            .gap_2()
                            .child(
                                Button::new(("reset-topic-offset", index))
                                    .label("Reset offset")
                                    .outline()
                                    .disabled(!mutable)
                                    .debug_selector(move || format!("topic-consumer-reset-{index}"))
                                    .on_click(cx.listener(move |_, _, _, cx| {
                                        cx.emit(TopicDetailIntent::ResetOffset {
                                            topic: reset_topic.clone(),
                                            consumer_group: reset_group.clone(),
                                            clusters: reset_clusters.clone(),
                                        });
                                    })),
                            )
                            .child(
                                Button::new(("skip-topic-accumulated", index))
                                    .label("Skip accumulated")
                                    .danger()
                                    .disabled(!mutable)
                                    .debug_selector(move || format!("topic-consumer-skip-{index}"))
                                    .on_click(cx.listener(move |_, _, _, cx| {
                                        cx.emit(TopicDetailIntent::SkipAccumulated {
                                            topic: skip_topic.clone(),
                                            consumer_group: skip_group.clone(),
                                            clusters: skip_clusters.clone(),
                                        });
                                    })),
                            )
                            .child(
                                Button::new(("open-topic-consumer", index))
                                    .label("Open Consumer")
                                    .outline()
                                    .on_click(cx.listener(move |_, _, _, cx| {
                                        if let Ok(group) = RouteKey::parse(group.clone()) {
                                            cx.emit(TopicDetailIntent::NavigateConsumer(AppRoute::ConsumerDetail {
                                                group,
                                                tab: ConsumerTab::Overview,
                                            }));
                                        }
                                    })),
                            ),
                    )
            }))
            .child(render_completeness(consumers.completeness, &consumers.failures, cx))
    }
}

impl Render for TopicDetail {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let selected = match self.store.active_tab {
            TopicTab::Overview => 0,
            TopicTab::Stats => 1,
            TopicTab::Route => 2,
            TopicTab::Configuration => 3,
            TopicTab::Consumers => 4,
        };
        let body = match self.store.active_tab {
            TopicTab::Overview => self.render_overview(cx),
            TopicTab::Stats => self.render_stats(cx),
            TopicTab::Route => self.render_route(cx),
            TopicTab::Configuration => self.render_configuration(cx),
            TopicTab::Consumers => self.render_consumers(cx),
        };
        div()
            .size_full()
            .flex()
            .flex_col()
            .gap_3()
            .when(self.store.selected.stale, |this| {
                match self.store.selected.inventory_completeness {
                    TopicCompleteness::Complete => {
                        this.child("Stale — this Topic is missing from the current complete inventory")
                    }
                    TopicCompleteness::Partial { .. } => this.child(
                        "Unverified — the current inventory is partial; absence is not authoritative and mutations are disabled",
                    ),
                }
            })
            .child(
                TabBar::new("topic-detail-tabs")
                    .selected_index(selected)
                    .children(["Overview", "Stats", "Route", "Configuration", "Consumers"])
                    .on_click(cx.listener(|detail, index, _, cx| {
                        let tab = match *index {
                            0 => TopicTab::Overview,
                            1 => TopicTab::Stats,
                            2 => TopicTab::Route,
                            3 => TopicTab::Configuration,
                            _ => TopicTab::Consumers,
                        };
                        detail.select_tab(tab, cx);
                    })),
            )
            .child(div().flex_1().min_h_0().overflow_y_scrollbar().child(body))
    }
}

fn render_resource<T>(
    state: &Loadable<T>,
    tab: TopicTab,
    empty_message: &'static str,
    cx: &mut Context<TopicDetail>,
    render_ready: impl Fn(&T, &mut Context<TopicDetail>) -> gpui::Div,
) -> gpui::Div {
    match state {
        Loadable::Idle | Loadable::InitialLoading => resource_status("Loading…", tab, false, cx),
        Loadable::Empty => resource_status(empty_message, tab, true, cx),
        Loadable::Ready(value) => render_ready(value, cx),
        Loadable::Refreshing(value) => div()
            .flex()
            .flex_col()
            .gap_2()
            .child(resource_status("Refreshing…", tab, false, cx))
            .child(render_ready(value, cx)),
        Loadable::Failed { previous, error } => {
            let status = resource_status(error.summary(), tab, true, cx);
            if let Some(value) = previous {
                div()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .child(status)
                    .child(render_ready(value, cx))
            } else {
                status
            }
        }
    }
}

fn resource_status(message: impl Into<String>, tab: TopicTab, retry: bool, cx: &mut Context<TopicDetail>) -> gpui::Div {
    let owner = cx.entity().downgrade();
    div()
        .p_4()
        .flex()
        .items_center()
        .justify_between()
        .gap_2()
        .text_sm()
        .text_color(cx.theme().muted_foreground)
        .child(message.into())
        .when(retry, |this| {
            this.child(
                Button::new(retry_button_id(tab))
                    .label("Retry")
                    .outline()
                    .on_click(move |_, _, cx| {
                        let _ = owner.update(cx, |detail, cx| detail.retry(tab, cx));
                    }),
            )
        })
}

const fn retry_button_id(tab: TopicTab) -> &'static str {
    match tab {
        TopicTab::Overview => "retry-topic-overview",
        TopicTab::Stats => "retry-topic-stats",
        TopicTab::Route => "retry-topic-route",
        TopicTab::Configuration => "retry-topic-configuration",
        TopicTab::Consumers => "retry-topic-consumers",
    }
}

fn render_completeness(
    completeness: TopicCompleteness,
    failures: &[TopicTargetFailure],
    cx: &mut Context<TopicDetail>,
) -> gpui::Div {
    let label = match completeness {
        TopicCompleteness::Complete => "Completeness: complete".to_owned(),
        TopicCompleteness::Partial {
            successful_target_count,
            failed_target_count,
        } => format!(
            "Completeness: partial · {successful_target_count} successful target(s) · {failed_target_count} failed target(s)"
        ),
    };
    div()
        .p_3()
        .border_1()
        .border_color(cx.theme().border)
        .rounded_md()
        .flex()
        .flex_col()
        .gap_1()
        .text_sm()
        .child(label)
        .children(failures.iter().map(|failure| {
            format!(
                "{} · stage={} · code={} · retryable={}",
                failure.target,
                failure_stage_label(failure.stage),
                failure_code_label(failure.code),
                failure.retryable
            )
        }))
}

const fn failure_stage_label(stage: TopicFailureStage) -> &'static str {
    match stage {
        TopicFailureStage::CatalogConfig => "catalog_config",
        TopicFailureStage::CatalogRoute => "catalog_route",
        TopicFailureStage::Stats => "stats",
        TopicFailureStage::Configuration => "configuration",
        TopicFailureStage::Consumer => "consumer",
        TopicFailureStage::Mutation => "mutation",
        TopicFailureStage::Reload => "reload",
    }
}

const fn failure_code_label(code: TopicFailureCode) -> &'static str {
    match code {
        TopicFailureCode::NotFound => "not_found",
        TopicFailureCode::InvalidData => "invalid_data",
        TopicFailureCode::Unavailable => "unavailable",
        TopicFailureCode::Conflict => "conflict",
    }
}

fn category_label(category: TopicCategory) -> &'static str {
    match category {
        TopicCategory::Application => "Application",
        TopicCategory::Retry => "Retry",
        TopicCategory::Dlq => "DLQ",
        TopicCategory::System => "System",
        TopicCategory::Unknown => "Unknown",
    }
}

fn message_type_label(message_type: TopicMessageType) -> &'static str {
    match message_type {
        TopicMessageType::Normal => "Normal",
        TopicMessageType::Delay => "Delay",
        TopicMessageType::Fifo => "FIFO",
        TopicMessageType::Transaction => "Transaction",
        TopicMessageType::Retry => "Retry",
        TopicMessageType::Dlq => "DLQ",
        TopicMessageType::System => "System",
        TopicMessageType::Unspecified => "Unspecified",
        TopicMessageType::Unknown => "Unknown",
    }
}

fn permission_label(permission: Option<TopicPermission>) -> String {
    permission.map_or_else(
        || "Unknown".into(),
        |permission| {
            format!(
                "{}{}{}",
                if permission.can_read() { "R" } else { "-" },
                if permission.can_write() { "W" } else { "-" },
                if permission.inherits() { "I" } else { "-" }
            )
        },
    )
}

fn config_field_label(field: &TopicConfigField) -> &'static str {
    match field {
        TopicConfigField::ReadQueues => "read queues",
        TopicConfigField::WriteQueues => "write queues",
        TopicConfigField::Permission => "permission",
        TopicConfigField::Ordered => "ordered",
        TopicConfigField::MessageType => "message type",
    }
}
