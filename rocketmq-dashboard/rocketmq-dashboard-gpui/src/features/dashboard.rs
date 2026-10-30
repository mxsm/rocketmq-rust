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
    AnyWindowHandle, AppContext as _, Context, Entity, EventEmitter, InteractiveElement as _, IntoElement,
    ParentElement as _, Render, Styled as _, Subscription, Task, Window, div, prelude::FluentBuilder as _, px,
};
use gpui_component::{
    ActiveTheme as _, Disableable as _, StyledExt as _,
    button::Button,
    calendar::Date,
    date_picker::{DatePicker, DatePickerEvent, DatePickerState},
    scroll::ScrollableElement as _,
    select::{SearchableVec, Select, SelectEvent, SelectState},
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

#[path = "dashboard_history.rs"]
mod history;
use history::{TopicHistoryOption, utc_day_range};

pub struct DashboardView {
    services: AppServices,
    revision: u64,
    store: DashboardStore,
    selected_topic: Option<String>,
    selected_broker: Option<BrokerIdentity>,
    history_date: chrono::NaiveDate,
    date_picker: Entity<DatePickerState>,
    topic_picker: Entity<SelectState<SearchableVec<TopicHistoryOption>>>,
    window: AnyWindowHandle,
    _subscriptions: Vec<Subscription>,
    broker_metric: HistoryMetricKind,
    topic_point_page: usize,
    broker_point_page: usize,
    health_task: Option<Task<()>>,
    overview_task: Option<Task<()>>,
    topic_current_task: Option<Task<()>>,
    broker_current_task: Option<Task<()>>,
    topic_history_task: Option<Task<()>>,
    broker_history_task: Option<Task<()>>,
}

impl EventEmitter<DashboardIntent> for DashboardView {}

impl DashboardView {
    pub fn new(window: &mut Window, services: AppServices, revision: u64, cx: &mut Context<Self>) -> Self {
        let history_date = chrono::Utc::now().date_naive();
        let date_picker = cx.new(|cx| {
            let mut state = DatePickerState::new(window, cx).date_format("%Y-%m-%d");
            state.set_date(history_date, window, cx);
            state
        });
        let topic_picker = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(vec![TopicHistoryOption(None)]),
                Some(gpui_component::IndexPath::default().row(0)),
                window,
                cx,
            )
            .searchable(true)
        });
        let subscriptions = vec![
            cx.subscribe(&date_picker, |view, _, event, cx| {
                if let DatePickerEvent::Change(Date::Single(Some(day))) = event {
                    view.select_history_day(*day, cx);
                }
            }),
            cx.subscribe(&topic_picker, |view, _, event, cx| {
                if let SelectEvent::Confirm(Some(topic)) = event {
                    view.select_history_topic(topic.clone(), cx);
                }
            }),
        ];
        let mut view = Self {
            services,
            revision,
            store: DashboardStore::default(),
            selected_topic: None,
            selected_broker: None,
            history_date,
            date_picker,
            topic_picker,
            window: window.window_handle(),
            _subscriptions: subscriptions,
            broker_metric: HistoryMetricKind::BrokerCount,
            topic_point_page: 0,
            broker_point_page: 0,
            health_task: None,
            overview_task: None,
            topic_current_task: None,
            broker_current_task: None,
            topic_history_task: None,
            broker_history_task: None,
        };
        view.refresh(cx);
        view.start_health_updates(cx);
        view
    }

    #[cfg(test)]
    pub(crate) fn topic_history_for_test(&self) -> &Loadable<Vec<rocketmq_dashboard_common::HistoryPoint>> {
        &self.store.topic_history.state
    }

    pub fn set_revision(&mut self, revision: u64, cx: &mut Context<Self>) {
        self.revision = revision;
        self.refresh(cx);
    }

    /// Discards the previous environment before its replacement provider is installed.
    pub fn clear_for_environment(&mut self, revision: u64, window: &mut Window, cx: &mut Context<Self>) {
        self.revision = revision;
        self.overview_task = None;
        self.topic_current_task = None;
        self.broker_current_task = None;
        self.topic_history_task = None;
        self.broker_history_task = None;
        self.store.overview.clear();
        self.store.topic_current.clear();
        self.store.broker_current.clear();
        self.store.topic_history.clear();
        self.store.broker_history.clear();
        self.selected_topic = None;
        self.selected_broker = None;
        self.sync_topic_picker(window, cx);
        self.store.health.clear();
        self.topic_point_page = 0;
        self.broker_point_page = 0;
        cx.notify();
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
        let window = self.window;
        self.topic_current_task = Some(cx.spawn(async move |this, cx| {
            let result = services.dashboard_topic_current(revision).await.map(Some);
            let _ = cx.update_window(window, |_, window, cx| {
                this.update(cx, |view, cx| {
                    if !view.store.topic_current.finish(request, view.revision, result) {
                        return;
                    }
                    if view.reconcile_topic_selection() {
                        view.store.topic_history.clear();
                        view.refresh_topic_history(cx);
                    }
                    view.sync_topic_picker(window, cx);
                    cx.notify();
                })
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
                if !view.store.broker_current.finish(request, view.revision, result) {
                    return;
                }
                if view.reconcile_broker_selection() && view.broker_metric != HistoryMetricKind::BrokerCount {
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
        let day = self.history_date;
        self.topic_history_task = Some(cx.spawn(async move |this, cx| {
            let result = async {
                let (start, end) = utc_day_range(day)?;
                let points = match topic {
                    Some(topic) => {
                        services
                            .dashboard_topic_history(request.revision(), topic, start, end)
                            .await?
                    }
                    None => {
                        services
                            .dashboard_count_history(request.revision(), HistoryMetricKind::TopicCount, start, end)
                            .await?
                    }
                };
                Ok((!points.is_empty()).then_some(points))
            }
            .await;
            let _ = this.update(cx, |view, cx| {
                if !view.store.topic_history.finish(request, view.revision, result) {
                    return;
                }
                let last_page = view
                    .store
                    .topic_history
                    .state
                    .value()
                    .map_or(0, |points| points.len().saturating_sub(1) / 24);
                view.topic_point_page = view.topic_point_page.min(last_page);
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
        let metric = self.broker_metric;
        let day = self.history_date;
        self.broker_history_task = Some(cx.spawn(async move |this, cx| {
            let result = async {
                let (start, end) = utc_day_range(day)?;
                let points = if metric == HistoryMetricKind::BrokerCount {
                    services
                        .dashboard_count_history(request.revision(), metric, start, end)
                        .await?
                } else if let Some(identity) = identity {
                    services
                        .dashboard_broker_history(request.revision(), metric, identity, start, end)
                        .await?
                } else {
                    return Ok(None);
                };
                Ok((!points.is_empty()).then_some(points))
            }
            .await;
            let _ = this.update(cx, |view, cx| {
                if !view.store.broker_history.finish(request, view.revision, result) {
                    return;
                }
                let last_page = view
                    .store
                    .broker_history
                    .state
                    .value()
                    .map_or(0, |points| points.len().saturating_sub(1) / 24);
                view.broker_point_page = view.broker_point_page.min(last_page);
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
        if self.selected_topic.as_ref().is_some_and(|selected| {
            !self
                .store
                .topic_current
                .state
                .value()
                .is_some_and(|load| load.available_topics.contains(selected))
        }) {
            self.selected_topic = None;
            return true;
        }
        false
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
                    "Partial data — {} overview observations are unavailable.",
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
                    .children(topics.into_iter().flat_map(|load| load.items.iter()).map(|item| {
                        div()
                            .py_2()
                            .flex()
                            .justify_between()
                            .child(item.topic.clone())
                            .child(div().flex().flex_col().items_end()
                                .child(format!("{} messages", observed_u64(item.total_messages)))
                                .child(div().text_xs().text_color(theme.muted_foreground)
                                    .child(format!("In: {} · Out: {}", observed_f64(item.produce_tps), observed_f64(item.consume_tps))))
                                .children(topics.into_iter().flat_map(|load| load.failures.iter())
                                    .filter(|failure| failure.topic == item.topic).map(|failure| {
                                        let stage = match failure.stage {
                                            rocketmq_dashboard_common::TopicCurrentStage::Rates => "Rates",
                                            rocketmq_dashboard_common::TopicCurrentStage::Offsets => "Offsets",
                                        };
                                        div().text_xs().text_color(theme.warning)
                                            .child(format!("{stage}: {} target observations unavailable", failure.failed_targets))
                                    })))
                    }))
                    .children(topics.filter(|load| load.failed_topics > 0 || load.omitted_topics > 0).map(|load|
                        div().text_sm().text_color(theme.warning).child(format!(
                            "Partial ranking: {} candidate Topics incomplete; {} application Topics outside the 200-candidate window. Up to 20 are sampled.",
                            load.failed_topics, load.omitted_topics))))
                    .children(topic_status)
                    .when(topics.is_some_and(|load| load.items.is_empty()), |this| {
                        this.child(div().text_sm().text_color(theme.muted_foreground)
                            .child("No application Topics are registered."))
                    })
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
        Loadable::Ready(_) => slot
            .last_updated_epoch_ms()
            .map(|updated| div().text_xs().text_color(muted).child(format_last_updated(updated))),
        Loadable::Idle | Loadable::InitialLoading | Loadable::Empty | Loadable::Failed { previous: None, .. } => None,
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

#[cfg(test)]
#[path = "dashboard_tests.rs"]
mod tests;
