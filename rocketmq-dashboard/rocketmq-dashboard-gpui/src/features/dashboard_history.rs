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

//! UTC day, metric and Topic controls plus collection quality and real point pages.

use gpui_component::select::SelectItem;
use rocketmq_dashboard_common::HistoryPoint;

use super::*;
use crate::infrastructure::history_health::CollectionStatus;

#[derive(Clone)]
pub(super) struct TopicHistoryOption(pub Option<String>);

impl SelectItem for TopicHistoryOption {
    type Value = Option<String>;
    fn title(&self) -> gpui::SharedString {
        self.0.clone().unwrap_or_else(|| "All topics".into()).into()
    }
    fn value(&self) -> &Self::Value {
        &self.0
    }
}

pub(super) fn utc_day_range(day: chrono::NaiveDate) -> Result<(u64, u64), UiError> {
    let start = day
        .and_hms_opt(0, 0, 0)
        .and_then(|time| u64::try_from(time.and_utc().timestamp_millis()).ok());
    start
        .and_then(|start| start.checked_add(86_400_000 - 1).map(|end| (start, end)))
        .ok_or_else(|| {
            UiError::new(
                "Select a UTC date on or after 1970-01-01.",
                UiErrorCode::Validation,
                false,
            )
        })
}

impl DashboardView {
    pub(super) fn start_health_updates(&mut self, cx: &mut Context<Self>) {
        self.health_task = Some(cx.spawn(async move |this, cx| {
            loop {
                let Ok((services, request)) = this.update(cx, |view, _| {
                    (view.services.clone(), view.store.health.begin(view.revision))
                }) else {
                    break;
                };
                if let Some(request) = request {
                    let result = services.dashboard_history_health(request.revision()).await.map(Some);
                    let _ = this.update(cx, |view, cx| {
                        view.store.health.finish(request, view.revision, result);
                        cx.notify();
                    });
                }
                cx.background_executor().timer(std::time::Duration::from_secs(5)).await;
            }
        }));
    }

    pub(super) fn sync_topic_picker(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let mut items = vec![TopicHistoryOption(None)];
        if let Some(load) = self.store.topic_current.state.value() {
            items.extend(
                load.available_topics
                    .iter()
                    .cloned()
                    .map(|topic| TopicHistoryOption(Some(topic))),
            );
        }
        let selected = self.selected_topic.clone();
        self.topic_picker.update(cx, |picker, cx| {
            picker.set_items(SearchableVec::new(items), window, cx);
            picker.set_selected_value(&selected, window, cx);
        });
    }

    pub(super) fn select_history_day(&mut self, day: chrono::NaiveDate, cx: &mut Context<Self>) {
        if self.history_date == day {
            return;
        }
        self.history_date = day;
        self.topic_point_page = 0;
        self.broker_point_page = 0;
        self.store.topic_history.clear();
        self.store.broker_history.clear();
        self.refresh_topic_history(cx);
        self.refresh_broker_history(cx);
        cx.notify();
    }

    pub(super) fn select_history_topic(&mut self, topic: Option<String>, cx: &mut Context<Self>) {
        if self.selected_topic == topic {
            return;
        }
        self.selected_topic = topic;
        self.topic_point_page = 0;
        self.store.topic_history.clear();
        self.refresh_topic_history(cx);
        cx.notify();
    }

    fn select_broker_metric(&mut self, metric: HistoryMetricKind, cx: &mut Context<Self>) {
        if self.broker_metric == metric {
            return;
        }
        self.broker_metric = metric;
        self.broker_point_page = 0;
        self.store.broker_history.clear();
        self.refresh_broker_history(cx);
        cx.notify();
    }

    fn select_next_broker(&mut self, cx: &mut Context<Self>) {
        let Some(brokers) = self
            .store
            .broker_current
            .state
            .value()
            .filter(|items| !items.is_empty())
        else {
            return;
        };
        let index = self
            .selected_broker
            .as_ref()
            .and_then(|selected| brokers.iter().position(|broker| &broker.identity == selected));
        self.selected_broker = Some(
            brokers[index.map_or(0, |index| (index + 1) % brokers.len())]
                .identity
                .clone(),
        );
        self.store.broker_history.clear();
        self.broker_point_page = 0;
        self.refresh_broker_history(cx);
        cx.notify();
    }

    pub(super) fn render_history(&self, cx: &mut Context<Self>) -> gpui::Div {
        let theme = cx.theme();
        let health = self.store.health.state.value().copied();
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let quality = health
            .map(|health| {
                let status = match health.status(now) {
                    CollectionStatus::Disabled => "Collection disabled",
                    CollectionStatus::Warming => "Warming up",
                    CollectionStatus::Normal => "Collecting normally",
                    CollectionStatus::Stale => "Stale collection",
                    CollectionStatus::Partial => "Partial collection",
                };
                let latest = health
                    .latest_success()
                    .map(format_timestamp)
                    .unwrap_or_else(|| "None this session".into());
                format!("{status} · Latest successful collection: {latest}")
            })
            .unwrap_or_else(|| "Collection status unavailable".into());
        div().flex().flex_col().gap_3()
            .child(div().text_sm().text_color(theme.muted_foreground).child(quality))
            .child(div().flex().flex_wrap().items_center().gap_2()
                .child(div().text_sm().child("UTC day (00:00–23:59:59.999)"))
                .child(div().id("dashboard-history-date").debug_selector(|| "dashboard-history-date".into())
                    .w(px(180.)).child(DatePicker::new(&self.date_picker)))
                .child(div().debug_selector(|| "dashboard-history-previous-day".into()).child(
                    Button::new("dashboard-history-previous-day").label("Previous day").outline()
                    .on_click(cx.listener(|view, _, window, cx| view.change_day(-1, window, cx)))))
                .child(div().debug_selector(|| "dashboard-history-next-day".into()).child(
                    Button::new("dashboard-history-next-day").label("Next day").outline()
                    .on_click(cx.listener(|view, _, window, cx| view.change_day(1, window, cx)))))
                .child(div().id("dashboard-history-topic").debug_selector(|| "dashboard-history-topic".into())
                    .w(px(240.)).child(Select::new(&self.topic_picker))))
            .child(div().text_xs().text_color(theme.muted_foreground)
                .child("History contains real observations only, up to the latest 1,440 points per query. Collection status describes this session."))
            .child(div().w_full().grid().grid_cols(2).gap_4()
                .child(div().debug_selector(|| "dashboard-topic-history-card".into()).min_w_0().flex().flex_col().gap_2()
                    .child(div().font_semibold().child(if self.selected_topic.is_some() { "Stored Topic messages · messages" } else { "All Topics · topics" }))
                    .child(history_body("topic", &self.store.topic_history, self.topic_point_page, theme.foreground, theme.muted_foreground, theme.border))
                    .child(self.point_controls(true, cx))
                    .child(div().debug_selector(|| "retry-topic-history".into()).child(
                        Button::new("retry-topic-history").label("Refresh Topic history").outline()
                        .on_click(cx.listener(|view, _, _, cx| view.refresh_topic_history(cx))))))
                .child(div().debug_selector(|| "dashboard-broker-history-card".into()).min_w_0().flex().flex_col().gap_2()
                    .child(div().font_semibold().child(format!("{} · {}", self.broker_metric.label(), self.broker_metric.unit())))
                    .child(div().flex().flex_wrap().gap_2().children([
                        ("dashboard-history-broker-count", "Count", HistoryMetricKind::BrokerCount),
                        ("dashboard-history-broker-produce", "Produce TPS", HistoryMetricKind::BrokerProduceTps),
                        ("dashboard-history-broker-consume", "Consume TPS", HistoryMetricKind::BrokerConsumeTps),
                    ].into_iter().map(|(id, label, metric)| div().debug_selector(move || id.into()).child(
                        Button::new(id).label(label).outline().disabled(self.broker_metric == metric)
                        .on_click(cx.listener(move |view, _, _, cx| view.select_broker_metric(metric, cx))))))
                    .when(self.broker_metric != HistoryMetricKind::BrokerCount, |this| this.child(
                        Button::new("dashboard-history-broker").label(self.selected_broker.as_ref()
                            .map(|broker| format!("{} / {} / {}", broker.cluster, broker.broker_name, broker.broker_id))
                            .unwrap_or_else(|| "No Broker available".into())).outline().disabled(self.selected_broker.is_none())
                            .on_click(cx.listener(|view, _, _, cx| view.select_next_broker(cx)))))
                    .child(history_body("broker", &self.store.broker_history, self.broker_point_page, theme.foreground, theme.muted_foreground, theme.border))
                    .child(self.point_controls(false, cx))
                    .child(div().debug_selector(|| "retry-broker-history".into()).child(
                        Button::new("retry-broker-history").label("Refresh Broker history").outline()
                        .on_click(cx.listener(|view, _, _, cx| view.refresh_broker_history(cx))))))))
    }

    fn change_day(&mut self, delta: i64, window: &mut Window, cx: &mut Context<Self>) {
        let Some(day) = self.history_date.checked_add_signed(chrono::TimeDelta::days(delta)) else {
            return;
        };
        if utc_day_range(day).is_err() {
            return;
        }
        self.date_picker
            .update(cx, |picker, cx| picker.set_date(day, window, cx));
        self.select_history_day(day, cx);
    }

    fn point_controls(&self, topic: bool, cx: &Context<Self>) -> gpui::Div {
        let (slot, page) = if topic {
            (&self.store.topic_history, self.topic_point_page)
        } else {
            (&self.store.broker_history, self.broker_point_page)
        };
        let count = slot.state.value().map_or(0, Vec::len);
        div()
            .flex()
            .items_center()
            .gap_2()
            .child(
                Button::new(("dashboard-points-newer", usize::from(topic)))
                    .label("Newer points")
                    .outline()
                    .disabled(page == 0)
                    .on_click(cx.listener(move |view, _, _, cx| {
                        let page = if topic {
                            &mut view.topic_point_page
                        } else {
                            &mut view.broker_point_page
                        };
                        *page = page.saturating_sub(1);
                        cx.notify();
                    })),
            )
            .child(
                Button::new(("dashboard-points-older", usize::from(topic)))
                    .label("Older points")
                    .outline()
                    .disabled((page + 1) * 24 >= count)
                    .on_click(cx.listener(move |view, _, _, cx| {
                        let page = if topic {
                            &mut view.topic_point_page
                        } else {
                            &mut view.broker_point_page
                        };
                        *page += 1;
                        cx.notify();
                    })),
            )
            .child(div().text_xs().child(format!("{} observed points", count)))
    }
}

fn format_timestamp(epoch_ms: u64) -> String {
    i64::try_from(epoch_ms)
        .ok()
        .and_then(chrono::DateTime::from_timestamp_millis)
        .map(|time| time.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| "Unknown time".into())
}

fn history_body(
    series: &'static str,
    slot: &ResourceSlot<Vec<HistoryPoint>>,
    page: usize,
    foreground: gpui::Hsla,
    muted: gpui::Hsla,
    border: gpui::Hsla,
) -> gpui::Div {
    let mut body = div().w_full().min_w_0().flex().flex_col().gap_2();
    if let Some(points) = slot.state.value() {
        body = body
            .child(
                div()
                    .debug_selector(move || format!("dashboard-{series}-history-chart"))
                    .w_full()
                    .min_w_0()
                    .child(trend::render(points, 150_000, foreground, muted, border)),
            )
            .children(
                points
                    .iter()
                    .rev()
                    .skip(page * 24)
                    .take(24)
                    .enumerate()
                    .map(|(index, point)| {
                        div()
                            .flex()
                            .flex_wrap()
                            .gap_x_4()
                            .gap_y_1()
                            .justify_between()
                            .text_xs()
                            .text_color(muted)
                            .child(
                                div()
                                    .debug_selector(move || format!("dashboard-{series}-history-time-{index}"))
                                    .flex_shrink_0()
                                    .whitespace_nowrap()
                                    .child(format_timestamp(point.timestamp_epoch_ms)),
                            )
                            .child(
                                div()
                                    .debug_selector(move || format!("dashboard-{series}-history-value-{index}"))
                                    .flex_shrink_0()
                                    .whitespace_nowrap()
                                    .text_right()
                                    .child(format!("{} {}", point.value, point.metric.unit())),
                            )
                    }),
            );
    }
    match &slot.state {
        Loadable::Idle | Loadable::InitialLoading => body.child("Loading…"),
        Loadable::Empty => body.child("No observations for this UTC day and selection."),
        Loadable::Refreshing(_) => body.child("Refreshing…"),
        Loadable::Ready(_) => body,
        Loadable::Failed { error, .. } => {
            body.child(div().text_sm().child(error.summary().to_owned()))
                .children(slot.last_updated_epoch_ms().map(|time| {
                    div()
                        .text_xs()
                        .child(format!("Last successful page load: {}", format_timestamp(time)))
                }))
        }
    }
}
