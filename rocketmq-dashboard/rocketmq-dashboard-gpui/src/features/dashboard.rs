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

//! Real five-resource Dashboard page.

use std::time::SystemTime;

use gpui::{
    Context, EventEmitter, InteractiveElement as _, IntoElement, ParentElement as _, Render, Styled as _, Task, Window,
    div, prelude::FluentBuilder as _, px,
};
use gpui_component::{
    ActiveTheme as _, Disableable as _, StyledExt as _, button::Button, scroll::ScrollableElement as _,
};
use rocketmq_dashboard_common::{BrokerIdentity, DashboardAction, HistoryMetricKind, Observed, dashboard_actions};

use crate::{
    components::{metric_card, trend},
    features::dashboard_store::{DashboardLayout, DashboardStore, ResourceSlot},
    route::AppRoute,
    services::{AppServices, brokers::BrokerCacheInvalidation},
    state::{Loadable, UiError, UiErrorCode},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DashboardIntent {
    Navigate(AppRoute),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum HistoryRange {
    Hour,
    #[default]
    Day,
    Week,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HistoryPresentation {
    Loading,
    Warming,
    Data,
    Refreshing,
    Error,
    DataWithWarning,
}

fn history_presentation<T>(state: &Loadable<T>) -> HistoryPresentation {
    match state {
        Loadable::Idle | Loadable::InitialLoading => HistoryPresentation::Loading,
        Loadable::Empty => HistoryPresentation::Warming,
        Loadable::Ready(_) => HistoryPresentation::Data,
        Loadable::Refreshing(_) => HistoryPresentation::Refreshing,
        Loadable::Failed { previous: None, .. } => HistoryPresentation::Error,
        Loadable::Failed { previous: Some(_), .. } => HistoryPresentation::DataWithWarning,
    }
}

impl HistoryRange {
    fn label(self) -> &'static str {
        match self {
            Self::Hour => "Last 1 hour",
            Self::Day => "Last 24 hours",
            Self::Week => "Last 7 days",
        }
    }

    fn duration_ms(self) -> u64 {
        match self {
            Self::Hour => 3_600_000,
            Self::Day => 86_400_000,
            Self::Week => 604_800_000,
        }
    }
}

pub struct DashboardView {
    services: AppServices,
    revision: u64,
    store: DashboardStore,
    selected_topic: Option<String>,
    selected_broker: Option<BrokerIdentity>,
    history_range: HistoryRange,
    overview_task: Option<Task<()>>,
    topic_current_task: Option<Task<()>>,
    broker_current_task: Option<Task<()>>,
    topic_history_task: Option<Task<()>>,
    broker_history_task: Option<Task<()>>,
}

impl EventEmitter<DashboardIntent> for DashboardView {}

impl DashboardView {
    pub fn new(services: AppServices, revision: u64, cx: &mut Context<Self>) -> Self {
        let mut view = Self {
            services,
            revision,
            store: DashboardStore::default(),
            selected_topic: None,
            selected_broker: None,
            history_range: HistoryRange::default(),
            overview_task: None,
            topic_current_task: None,
            broker_current_task: None,
            topic_history_task: None,
            broker_history_task: None,
        };
        view.refresh(cx);
        view
    }

    pub fn set_revision(&mut self, revision: u64, cx: &mut Context<Self>) {
        self.revision = revision;
        self.refresh(cx);
    }

    pub fn consume_invalidations(&mut self, invalidations: &[BrokerCacheInvalidation], cx: &mut Context<Self>) {
        if invalidations.contains(&BrokerCacheInvalidation::DashboardOverview) {
            self.store.overview.invalidate();
            self.refresh_overview(cx);
        }
        if invalidations.contains(&BrokerCacheInvalidation::DashboardBrokerCurrent) {
            self.store.broker_current.invalidate();
            self.refresh_broker_current(cx);
        }
    }

    pub fn refresh(&mut self, cx: &mut Context<Self>) {
        self.refresh_overview(cx);
        self.refresh_topic_current(cx);
        self.refresh_broker_current(cx);
        self.refresh_topic_history(cx);
        self.refresh_broker_history(cx);
        cx.notify();
    }

    fn refresh_overview(&mut self, cx: &mut Context<Self>) {
        let Some(request) = self.store.overview.begin(self.revision) else {
            return;
        };
        let services = self.services.clone();
        let revision = self.revision;
        self.overview_task = Some(cx.spawn(async move |this, cx| {
            let result = services.dashboard_overview(revision).await.map(Some);
            let _ = this.update(cx, |view, cx| {
                view.store.overview.finish(request, view.revision, result);
                cx.notify();
            });
        }));
    }

    fn refresh_topic_current(&mut self, cx: &mut Context<Self>) {
        let Some(request) = self.store.topic_current.begin(self.revision) else {
            return;
        };
        let services = self.services.clone();
        let revision = self.revision;
        self.topic_current_task = Some(cx.spawn(async move |this, cx| {
            let result = services
                .dashboard_topic_current(revision)
                .await
                .map(|items| (!items.is_empty()).then_some(items));
            let _ = this.update(cx, |view, cx| {
                view.store.topic_current.finish(request, view.revision, result);
                if view.reconcile_topic_selection() {
                    view.store.topic_history.invalidate();
                    view.refresh_topic_history(cx);
                }
                cx.notify();
            });
        }));
    }

    fn refresh_broker_current(&mut self, cx: &mut Context<Self>) {
        let Some(request) = self.store.broker_current.begin(self.revision) else {
            return;
        };
        let services = self.services.clone();
        let revision = self.revision;
        self.broker_current_task = Some(cx.spawn(async move |this, cx| {
            let result = services
                .dashboard_broker_current(revision)
                .await
                .map(|items| (!items.is_empty()).then_some(items));
            let _ = this.update(cx, |view, cx| {
                view.store.broker_current.finish(request, view.revision, result);
                if view.reconcile_broker_selection() {
                    view.store.broker_history.invalidate();
                    view.refresh_broker_history(cx);
                }
                cx.notify();
            });
        }));
    }

    fn refresh_topic_history(&mut self, cx: &mut Context<Self>) {
        let Some(request) = self.store.topic_history.begin(self.revision) else {
            return;
        };
        let services = self.services.clone();
        let topic = self.selected_topic.clone();
        let range = self.history_range;
        self.topic_history_task = Some(cx.spawn(async move |this, cx| {
            let result = async {
                let Some(topic) = topic else {
                    return Ok(None);
                };
                let (start, end) = history_range(range)?;
                services
                    .dashboard_topic_history(topic, start, end)
                    .await
                    .map(|points| (!points.is_empty()).then_some(points))
            }
            .await;
            let _ = this.update(cx, |view, cx| {
                view.store.topic_history.finish(request, view.revision, result);
                cx.notify();
            });
        }));
    }

    fn refresh_broker_history(&mut self, cx: &mut Context<Self>) {
        let Some(request) = self.store.broker_history.begin(self.revision) else {
            return;
        };
        let services = self.services.clone();
        let identity = self.selected_broker.clone();
        let range = self.history_range;
        self.broker_history_task = Some(cx.spawn(async move |this, cx| {
            let result = async {
                let Some(identity) = identity else {
                    return Ok(None);
                };
                let (start, end) = history_range(range)?;
                services
                    .dashboard_broker_history(HistoryMetricKind::BrokerProduceTps, identity, start, end)
                    .await
                    .map(|points| (!points.is_empty()).then_some(points))
            }
            .await;
            let _ = this.update(cx, |view, cx| {
                view.store.broker_history.finish(request, view.revision, result);
                cx.notify();
            });
        }));
    }

    fn emit_action(&mut self, action: DashboardAction, cx: &mut Context<Self>) {
        let route = match action {
            DashboardAction::OpenOperations => AppRoute::OpsSettings,
            DashboardAction::OpenBrokers => AppRoute::Brokers,
        };
        cx.emit(DashboardIntent::Navigate(route));
    }

    fn reconcile_topic_selection(&mut self) -> bool {
        let topics = self.store.topic_current.state.value();
        let selected_is_present = self
            .selected_topic
            .as_ref()
            .is_some_and(|selected| topics.is_some_and(|items| items.iter().any(|item| &item.topic == selected)));
        if selected_is_present {
            return false;
        }
        let next = topics.and_then(|items| items.first()).map(|item| item.topic.clone());
        let changed = self.selected_topic != next;
        self.selected_topic = next;
        changed
    }

    fn reconcile_broker_selection(&mut self) -> bool {
        let brokers = self.store.broker_current.state.value();
        let next = match (&self.selected_broker, brokers) {
            (Some(selected), Some(brokers)) if brokers.iter().any(|item| &item.identity == selected) => {
                Some(selected.clone())
            }
            (_, Some(brokers)) => brokers.first().map(|item| item.identity.clone()),
            (_, None) => None,
        };
        let changed = next != self.selected_broker;
        self.selected_broker = next;
        changed
    }

    fn select_next_topic(&mut self, cx: &mut Context<Self>) {
        let Some(items) = self.store.topic_current.state.value() else {
            return;
        };
        if items.is_empty() {
            return;
        }
        let current = self
            .selected_topic
            .as_ref()
            .and_then(|selected| items.iter().position(|item| &item.topic == selected))
            .unwrap_or(0);
        self.selected_topic = Some(items[(current + 1) % items.len()].topic.clone());
        self.store.topic_history.invalidate();
        self.refresh_topic_history(cx);
    }

    fn cycle_history_range(&mut self, cx: &mut Context<Self>) {
        self.history_range = match self.history_range {
            HistoryRange::Hour => HistoryRange::Day,
            HistoryRange::Day => HistoryRange::Week,
            HistoryRange::Week => HistoryRange::Hour,
        };
        self.store.topic_history.invalidate();
        self.store.broker_history.invalidate();
        self.refresh_topic_history(cx);
        self.refresh_broker_history(cx);
    }

    fn render_overview(&self, layout: DashboardLayout, cx: &mut Context<Self>) -> gpui::Div {
        let theme = cx.theme();
        let metric_min_width = match layout {
            DashboardLayout::Wide { .. } => px(190.),
            DashboardLayout::Compact { .. } => px(300.),
        };
        let Some(load) = self.store.overview.state.value() else {
            return resource_message("Overview", &self.store.overview.state, theme.muted_foreground);
        };
        let status = retained_resource_status(
            "Overview",
            &self.store.overview,
            "dashboard-overview-refreshing",
            "dashboard-overview-error",
            "dashboard-overview-last-updated",
            "retry-dashboard-overview",
            Button::new("retry-dashboard-overview")
                .label("Retry")
                .outline()
                .on_click(cx.listener(|view, _, _, cx| view.refresh_overview(cx))),
            theme.muted_foreground,
            theme.danger,
        );
        let overview = &load.overview;
        div()
            .flex()
            .flex_col()
            .gap_3()
            .child(
                div()
                    .flex()
                    .flex_wrap()
                    .gap_3()
                    .child(
                        metric_card::render(
                            "Brokers",
                            observed_u64(overview.broker_count),
                            theme.foreground,
                            theme.muted_foreground,
                            theme.border,
                        )
                        .min_w(metric_min_width),
                    )
                    .child(
                        metric_card::render(
                            "Topics",
                            observed_u64(overview.topic_count),
                            theme.foreground,
                            theme.muted_foreground,
                            theme.border,
                        )
                        .min_w(metric_min_width),
                    )
                    .child(
                        metric_card::render(
                            "Consumers",
                            observed_u64(overview.consumer_group_count),
                            theme.foreground,
                            theme.muted_foreground,
                            theme.border,
                        )
                        .min_w(metric_min_width),
                    )
                    .child(
                        metric_card::render(
                            "Backlog",
                            observed_i64(overview.consumer_backlog),
                            theme.foreground,
                            theme.muted_foreground,
                            theme.border,
                        )
                        .min_w(metric_min_width),
                    ),
            )
            .when(load.has_warning(), |this| {
                this.child(div().text_sm().text_color(theme.warning).child(format!(
                    "Partial data — {} overview resources could not be loaded.",
                    load.failed_resources
                )))
            })
            .children(status)
            .child(
                div()
                    .flex()
                    .gap_2()
                    .children(
                        dashboard_actions(overview)
                            .into_iter()
                            .enumerate()
                            .map(|(index, action)| {
                                let label = match action {
                                    DashboardAction::OpenOperations => "Open Operations",
                                    DashboardAction::OpenBrokers => "Open Brokers",
                                };
                                Button::new(("dashboard-action", index))
                                    .label(label)
                                    .outline()
                                    .on_click(cx.listener(move |view, _, _, cx| view.emit_action(action.clone(), cx)))
                            }),
                    ),
            )
    }

    fn render_current(&self, cx: &mut Context<Self>) -> gpui::Div {
        let theme = cx.theme();
        let topics = self.store.topic_current.state.value();
        let brokers = self.store.broker_current.state.value();
        let topic_status = retained_resource_status(
            "Topic Current",
            &self.store.topic_current,
            "dashboard-topic-current-refreshing",
            "dashboard-topic-current-error",
            "dashboard-topic-current-last-updated",
            "retry-dashboard-topic-current",
            Button::new("retry-dashboard-topic-current")
                .label("Retry")
                .outline()
                .on_click(cx.listener(|view, _, _, cx| view.refresh_topic_current(cx))),
            theme.muted_foreground,
            theme.danger,
        );
        let broker_status = retained_resource_status(
            "Broker Current",
            &self.store.broker_current,
            "dashboard-broker-current-refreshing",
            "dashboard-broker-current-error",
            "dashboard-broker-current-last-updated",
            "retry-dashboard-broker-current",
            Button::new("retry-dashboard-broker-current")
                .label("Retry")
                .outline()
                .on_click(cx.listener(|view, _, _, cx| view.refresh_broker_current(cx))),
            theme.muted_foreground,
            theme.danger,
        );
        div()
            .flex()
            .flex_wrap()
            .gap_4()
            .child(
                div()
                    .flex_1()
                    .min_w(px(320.))
                    .p_4()
                    .rounded_lg()
                    .border_1()
                    .border_color(theme.border)
                    .child(div().font_semibold().child("Topic Current"))
                    .children(topics.into_iter().flat_map(|items| items.iter().take(8)).map(|item| {
                        div()
                            .py_2()
                            .flex()
                            .justify_between()
                            .child(item.topic.clone())
                            .child(observed_u64(item.total_messages))
                    }))
                    .children(topic_status)
                    .when(topics.is_none(), |this| {
                        this.child(resource_message(
                            "Topic Current",
                            &self.store.topic_current.state,
                            theme.muted_foreground,
                        ))
                    }),
            )
            .child(
                div()
                    .flex_1()
                    .min_w(px(320.))
                    .p_4()
                    .rounded_lg()
                    .border_1()
                    .border_color(theme.border)
                    .child(div().font_semibold().child("Broker Current"))
                    .children(brokers.into_iter().flat_map(|items| items.iter().take(8)).map(|item| {
                        div()
                            .py_2()
                            .flex()
                            .justify_between()
                            .child(item.identity.broker_name.clone())
                            .child(observed_f64(item.combined_tps))
                    }))
                    .children(broker_status)
                    .when(brokers.is_none(), |this| {
                        this.child(resource_message(
                            "Broker Current",
                            &self.store.broker_current.state,
                            theme.muted_foreground,
                        ))
                    }),
            )
    }

    fn render_history(&self, cx: &mut Context<Self>) -> gpui::Div {
        let theme = cx.theme();
        div()
            .flex()
            .flex_col()
            .gap_3()
            .child(
                div()
                    .flex()
                    .flex_wrap()
                    .justify_between()
                    .gap_2()
                    .child(
                        Button::new("dashboard-history-topic")
                            .label(
                                self.selected_topic
                                    .clone()
                                    .unwrap_or_else(|| "No Topic available".into()),
                            )
                            .outline()
                            .disabled(self.selected_topic.is_none())
                            .on_click(cx.listener(|view, _, _, cx| view.select_next_topic(cx))),
                    )
                    .child(
                        Button::new("dashboard-history-range")
                            .label(self.history_range.label())
                            .outline()
                            .on_click(cx.listener(|view, _, _, cx| view.cycle_history_range(cx))),
                    ),
            )
            .child(
                div()
                    .flex()
                    .flex_wrap()
                    .gap_4()
                    .child(
                        div()
                            .flex_1()
                            .min_w(px(320.))
                            .child(div().font_semibold().child("Topic History"))
                            .child(history_body(
                                &self.store.topic_history.state,
                                150_000,
                                theme.foreground,
                                theme.muted_foreground,
                                theme.border,
                            ))
                            .when(
                                matches!(
                                    history_presentation(&self.store.topic_history.state),
                                    HistoryPresentation::Error | HistoryPresentation::DataWithWarning
                                ),
                                |this| {
                                    this.child(
                                        Button::new("retry-topic-history")
                                            .label("Retry")
                                            .outline()
                                            .on_click(cx.listener(|view, _, _, cx| view.refresh_topic_history(cx))),
                                    )
                                },
                            ),
                    )
                    .child(
                        div()
                            .flex_1()
                            .min_w(px(320.))
                            .child(div().font_semibold().child("Broker History"))
                            .child(history_body(
                                &self.store.broker_history.state,
                                150_000,
                                theme.foreground,
                                theme.muted_foreground,
                                theme.border,
                            ))
                            .when(
                                matches!(
                                    history_presentation(&self.store.broker_history.state),
                                    HistoryPresentation::Error | HistoryPresentation::DataWithWarning
                                ),
                                |this| {
                                    this.child(
                                        Button::new("retry-broker-history")
                                            .label("Retry")
                                            .outline()
                                            .on_click(cx.listener(|view, _, _, cx| view.refresh_broker_history(cx))),
                                    )
                                },
                            ),
                    ),
            )
    }
}

impl Render for DashboardView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let layout = DashboardLayout::for_width(f32::from(window.viewport_size().width));
        div()
            .size_full()
            .px_6()
            .pb_6()
            .flex()
            .flex_col()
            .gap_4()
            .child(
                div().flex().justify_end().child(
                    Button::new("refresh-dashboard")
                        .label("Refresh")
                        .outline()
                        .on_click(cx.listener(|view, _, _, cx| view.refresh(cx))),
                ),
            )
            .child(
                div().flex_1().min_h_0().overflow_y_scrollbar().child(
                    div()
                        .flex()
                        .flex_col()
                        .gap_6()
                        .child(self.render_overview(layout, cx))
                        .child(self.render_current(cx))
                        .child(self.render_history(cx)),
                ),
            )
    }
}

fn resource_message<T>(title: &str, state: &Loadable<T>, color: gpui::Hsla) -> gpui::Div {
    let message = match state {
        Loadable::Idle | Loadable::InitialLoading => "Loading…".to_owned(),
        Loadable::Refreshing(_) => "Refreshing…".to_owned(),
        Loadable::Empty => "No data returned.".to_owned(),
        Loadable::Failed { error, .. } => error.summary().to_owned(),
        Loadable::Ready(_) => String::new(),
    };
    div()
        .p_4()
        .text_sm()
        .text_color(color)
        .child(format!("{title}: {message}"))
}

#[allow(
    clippy::too_many_arguments,
    reason = "The three product resources use distinct stable test and focus IDs."
)]
fn retained_resource_status<T>(
    title: &str,
    slot: &ResourceSlot<T>,
    refreshing_id: &'static str,
    error_id: &'static str,
    last_updated_id: &'static str,
    retry_id: &'static str,
    retry: Button,
    muted: gpui::Hsla,
    danger: gpui::Hsla,
) -> Option<gpui::Div> {
    match &slot.state {
        Loadable::Refreshing(_) => Some(
            div().child(
                div()
                    .debug_selector(|| refreshing_id.to_owned())
                    .id(refreshing_id)
                    .flex()
                    .items_center()
                    .gap_2()
                    .text_sm()
                    .text_color(muted)
                    .child(format!("{title}: Refreshing…"))
                    .children(slot.last_updated_epoch_ms().map(|updated| {
                        div()
                            .debug_selector(|| last_updated_id.to_owned())
                            .id(last_updated_id)
                            .child(format_last_updated(updated))
                    })),
            ),
        ),
        Loadable::Failed {
            previous: Some(_),
            error,
        } => Some(
            div().child(
                div()
                    .debug_selector(|| error_id.to_owned())
                    .id(error_id)
                    .flex()
                    .flex_wrap()
                    .items_center()
                    .gap_2()
                    .text_sm()
                    .text_color(danger)
                    .child(format!("{title}: {}", error.summary()))
                    .children(slot.last_updated_epoch_ms().map(|updated| {
                        div()
                            .debug_selector(|| last_updated_id.to_owned())
                            .id(last_updated_id)
                            .text_color(muted)
                            .child(format_last_updated(updated))
                    }))
                    .child(div().debug_selector(|| retry_id.to_owned()).child(retry)),
            ),
        ),
        Loadable::Idle
        | Loadable::InitialLoading
        | Loadable::Ready(_)
        | Loadable::Empty
        | Loadable::Failed { previous: None, .. } => None,
    }
}

fn format_last_updated(epoch_ms: u64) -> String {
    let elapsed_seconds = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .map(|now| now.saturating_sub(epoch_ms) / 1_000)
        .unwrap_or_default();
    match elapsed_seconds {
        0..=59 => "Last updated: just now".into(),
        60..=3_599 => format!("Last updated: {}m ago", elapsed_seconds / 60),
        _ => format!("Last updated: {}h ago", elapsed_seconds / 3_600),
    }
}

fn observed_u64(value: Observed<u64>) -> String {
    match value {
        Observed::Observed(value) => value.to_string(),
        Observed::Unknown => "Unknown".into(),
    }
}

fn observed_i64(value: Observed<i64>) -> String {
    match value {
        Observed::Observed(value) => value.to_string(),
        Observed::Unknown => "Unknown".into(),
    }
}

fn observed_f64(value: Observed<f64>) -> String {
    match value {
        Observed::Observed(value) => format!("{value:.2} TPS"),
        Observed::Unknown => "Unknown".into(),
    }
}

fn history_body(
    state: &Loadable<Vec<rocketmq_dashboard_common::HistoryPoint>>,
    max_gap_ms: u64,
    foreground: gpui::Hsla,
    muted: gpui::Hsla,
    border: gpui::Hsla,
) -> gpui::Div {
    match state {
        Loadable::Idle | Loadable::InitialLoading => div().p_4().text_sm().text_color(muted).child("Loading…"),
        Loadable::Empty => trend::render(&[], max_gap_ms, foreground, muted, border),
        Loadable::Ready(points) => trend::render(points, max_gap_ms, foreground, muted, border),
        Loadable::Refreshing(points) => div()
            .flex()
            .flex_col()
            .gap_2()
            .child(div().text_sm().text_color(muted).child("Refreshing…"))
            .child(trend::render(points, max_gap_ms, foreground, muted, border)),
        Loadable::Failed {
            previous: Some(points),
            error,
        } => div()
            .flex()
            .flex_col()
            .gap_2()
            .child(div().text_sm().text_color(muted).child(error.summary().to_owned()))
            .child(trend::render(points, max_gap_ms, foreground, muted, border)),
        Loadable::Failed { previous: None, error } => div()
            .p_4()
            .text_sm()
            .text_color(muted)
            .child(error.summary().to_owned()),
    }
}

fn history_range(range: HistoryRange) -> Result<(u64, u64), UiError> {
    let end = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .ok_or_else(|| UiError::new("System time is unavailable.", UiErrorCode::Unknown, true))?;
    Ok((end.saturating_sub(range.duration_ms()), end))
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc, sync::Arc};

    use gpui::{AppContext as _, point, px, size};
    use gpui_component::Root;
    use rocketmq_dashboard_common::{
        BrokerCurrentMetric, BrokerIdentity, DashboardOverview, HistoryPoint, TopicCurrentMetric,
    };

    use super::*;
    use crate::services::{dashboard::DashboardOverviewLoad, delivery03::test_support::FakeDelivery03Backend};

    fn error() -> UiError {
        UiError::new("History unavailable", UiErrorCode::Connection, true)
    }

    #[test]
    fn failed_history_is_error_never_warming_and_refresh_retains_data() {
        let failed: Loadable<Vec<u64>> = Loadable::Failed {
            previous: None,
            error: error(),
        };
        assert_eq!(history_presentation(&failed), HistoryPresentation::Error);
        assert_ne!(history_presentation(&failed), HistoryPresentation::Warming);
        let refreshing = Loadable::Refreshing(vec![1]);
        assert_eq!(history_presentation(&refreshing), HistoryPresentation::Refreshing);
        let retained_failure = Loadable::Failed {
            previous: Some(vec![1]),
            error: error(),
        };
        assert_eq!(
            history_presentation(&retained_failure),
            HistoryPresentation::DataWithWarning
        );
    }

    #[test]
    fn history_range_filter_uses_exact_selected_duration() {
        assert_eq!(HistoryRange::Hour.duration_ms(), 3_600_000);
        assert_eq!(HistoryRange::Day.duration_ms(), 86_400_000);
        assert_eq!(HistoryRange::Week.duration_ms(), 604_800_000);
    }

    #[gpui::test]
    fn retained_dashboard_resources_render_refresh_and_inline_error_retry_states(cx: &mut gpui::TestAppContext) {
        cx.update(gpui_component::init);
        let services = AppServices::default().with_delivery03_backend(Arc::new(FakeDelivery03Backend::default()));
        let capture = Rc::new(RefCell::new(None));
        let capture_view = capture.clone();
        let (root, cx) = cx.add_window_view(move |window, cx| {
            let view = cx.new(|cx| DashboardView::new(services, 11, cx));
            capture_view.replace(Some(view.clone()));
            Root::new(view, window, cx)
        });
        let view = capture.borrow_mut().take().expect("Dashboard entity");
        cx.run_until_parked();

        let (overview_request, topic_request, broker_request) = cx.update(|_, app| {
            view.update(app, |view, cx| {
                let overview = view.store.overview.begin(11).expect("overview ready request");
                assert!(view.store.overview.finish(
                    overview,
                    11,
                    Ok(Some(DashboardOverviewLoad {
                        overview: DashboardOverview::default(),
                        failed_resources: 0,
                    }))
                ));
                let topic = view.store.topic_current.begin(11).expect("topic ready request");
                assert!(view.store.topic_current.finish(
                    topic,
                    11,
                    Ok(Some(vec![TopicCurrentMetric {
                        topic: "orders".into(),
                        total_messages: Observed::Observed(7),
                        produce_tps: Observed::Unknown,
                        consume_tps: Observed::Unknown,
                    }]))
                ));
                let broker = view.store.broker_current.begin(11).expect("broker ready request");
                assert!(view.store.broker_current.finish(
                    broker,
                    11,
                    Ok(Some(vec![BrokerCurrentMetric::observed(
                        BrokerIdentity {
                            cluster: "cluster-a".into(),
                            broker_name: "broker-a".into(),
                            broker_id: 0,
                            address: "127.0.0.1:10911".into(),
                        },
                        "5.3.2".into(),
                        1.0,
                        2.0,
                    )]))
                ));
                assert!(view.store.overview.last_updated_epoch_ms().is_some());
                assert!(view.store.topic_current.last_updated_epoch_ms().is_some());
                assert!(view.store.broker_current.last_updated_epoch_ms().is_some());
                let requests = (
                    view.store.overview.begin(11).expect("overview refresh"),
                    view.store.topic_current.begin(11).expect("topic refresh"),
                    view.store.broker_current.begin(11).expect("broker refresh"),
                );
                cx.notify();
                requests
            })
        });

        cx.simulate_resize(size(px(1_440.), px(900.)));
        cx.draw(point(px(0.), px(0.)), size(px(1_440.), px(900.)), |_, _| root.clone());
        cx.read(|app| {
            let view = view.read(app);
            assert!(matches!(view.store.overview.state, Loadable::Refreshing(_)));
            assert!(matches!(view.store.topic_current.state, Loadable::Refreshing(_)));
            assert!(matches!(view.store.broker_current.state, Loadable::Refreshing(_)));
        });
        for selector in [
            "dashboard-overview-refreshing",
            "dashboard-topic-current-refreshing",
            "dashboard-broker-current-refreshing",
        ] {
            assert!(cx.debug_bounds(selector).is_some(), "missing refresh state: {selector}");
        }

        cx.update(|_, app| {
            view.update(app, |view, cx| {
                let error = UiError::new("Current metrics unavailable", UiErrorCode::Connection, true);
                assert!(view.store.overview.finish(overview_request, 11, Err(error.clone())));
                assert!(view.store.topic_current.finish(topic_request, 11, Err(error.clone())));
                assert!(view.store.broker_current.finish(broker_request, 11, Err(error)));
                cx.notify();
            });
        });
        cx.draw(point(px(0.), px(0.)), size(px(1_440.), px(900.)), |_, _| root.clone());
        for selector in [
            "dashboard-overview-error",
            "dashboard-overview-last-updated",
            "retry-dashboard-overview",
            "dashboard-topic-current-error",
            "dashboard-topic-current-last-updated",
            "retry-dashboard-topic-current",
            "dashboard-broker-current-error",
            "dashboard-broker-current-last-updated",
            "retry-dashboard-broker-current",
        ] {
            assert!(
                cx.debug_bounds(selector).is_some(),
                "missing retained error state: {selector}"
            );
        }
        cx.read(|app| {
            let view = view.read(app);
            assert!(matches!(
                view.store.overview.state,
                Loadable::Failed { previous: Some(_), .. }
            ));
            assert!(view.store.topic_current.state.value().is_some());
            assert!(view.store.broker_current.state.value().is_some());
        });
    }

    #[gpui::test]
    fn history_failure_and_topic_range_filters_stay_on_the_history_product_path(cx: &mut gpui::TestAppContext) {
        cx.update(gpui_component::init);
        let fake = Arc::new(FakeDelivery03Backend::default());
        fake.queue_topic_current(Ok(vec![
            TopicCurrentMetric {
                topic: "orders-a".into(),
                total_messages: Observed::Observed(1),
                produce_tps: Observed::Unknown,
                consume_tps: Observed::Unknown,
            },
            TopicCurrentMetric {
                topic: "orders-b".into(),
                total_messages: Observed::Observed(2),
                produce_tps: Observed::Unknown,
                consume_tps: Observed::Unknown,
            },
        ]));
        fake.queue_topic_history(Err(error()));
        fake.queue_topic_history(Ok(vec![HistoryPoint {
            metric: HistoryMetricKind::TopicMessages,
            series_identity: "orders-b".into(),
            timestamp_epoch_ms: 2,
            value: 2.0,
            source_revision: 9,
        }]));
        fake.queue_topic_history(Ok(Vec::new()));
        let broker_identity = BrokerIdentity {
            cluster: "cluster-a".into(),
            broker_name: "broker-a".into(),
            broker_id: 0,
            address: "127.0.0.1:10911".into(),
        };
        fake.queue_broker_current(Ok(vec![rocketmq_dashboard_common::BrokerCurrentMetric::observed(
            broker_identity.clone(),
            "5.3.2".into(),
            1.0,
            2.0,
        )]));
        for timestamp_epoch_ms in [1, 2] {
            fake.queue_broker_history(Ok(vec![HistoryPoint {
                metric: HistoryMetricKind::BrokerProduceTps,
                series_identity: rocketmq_dashboard_common::broker_history_series_identity(&broker_identity),
                timestamp_epoch_ms,
                value: timestamp_epoch_ms as f64,
                source_revision: 9,
            }]));
        }
        let services = AppServices::default().with_delivery03_backend(fake.clone());
        let (view, cx) = cx.add_window_view(move |_, cx| DashboardView::new(services, 9, cx));
        cx.run_until_parked();

        cx.read(|app| {
            let view = view.read(app);
            assert_eq!(view.selected_topic.as_deref(), Some("orders-a"));
            assert_eq!(
                history_presentation(&view.store.topic_history.state),
                HistoryPresentation::Error
            );
            assert_ne!(
                history_presentation(&view.store.topic_history.state),
                HistoryPresentation::Warming
            );
        });
        let baseline = fake.calls();
        assert_eq!(baseline.overview_revisions, [9]);
        assert_eq!(baseline.topic_current_revisions, [9]);
        assert_eq!(baseline.broker_current_revisions, [9]);
        assert_eq!(baseline.topic_history.len(), 1);
        assert_eq!(baseline.broker_history.len(), 1);

        cx.update(|_, app| view.update(app, |view, cx| view.select_next_topic(cx)));
        cx.run_until_parked();
        cx.read(|app| {
            let view = view.read(app);
            assert_eq!(view.selected_topic.as_deref(), Some("orders-b"));
            assert!(matches!(view.store.topic_history.state, Loadable::Ready(_)));
        });

        cx.update(|_, app| view.update(app, |view, cx| view.cycle_history_range(cx)));
        cx.run_until_parked();
        let calls = fake.calls();
        assert_eq!(calls.topic_history.len(), 3);
        assert_eq!(calls.topic_history[0].0, "orders-a");
        assert_eq!(calls.topic_history[1].0, "orders-b");
        assert_eq!(calls.overview_revisions, [9]);
        assert_eq!(calls.topic_current_revisions, [9]);
        assert_eq!(calls.broker_current_revisions, [9]);
        assert_eq!(calls.broker_history.len(), 2);
        assert_eq!(
            calls.topic_history[2].2.saturating_sub(calls.topic_history[2].1),
            604_800_000
        );
    }
}
