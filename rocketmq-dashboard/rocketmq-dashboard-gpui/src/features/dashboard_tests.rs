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

//! Dashboard controls and retained-data regressions.

use std::{cell::RefCell, rc::Rc, sync::Arc};

use gpui::{AppContext as _, point, px, size};
use gpui_component::Root;
use rocketmq_dashboard_common::{
    BrokerCurrentMetric, BrokerIdentity, DashboardOverview, HistoryPoint, TopicCurrentLoad, TopicCurrentObservation,
};

use super::*;
use crate::services::{
    dashboard::DashboardOverviewLoad, dashboard_broker::test_support::FakeDashboardBrokerOperations,
};

fn error() -> UiError {
    UiError::new("History unavailable", UiErrorCode::Connection, true)
}

#[gpui::test]
fn environment_clear_discards_all_dashboard_values_and_rejects_old_requests(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    let (view, cx) = cx.add_window_view(|window, cx| DashboardView::new(window, AppServices::default(), 10, cx));
    cx.run_until_parked();
    cx.update(|window, app| {
        view.update(app, |view, cx| {
            view.store.overview.replace(DashboardOverviewLoad {
                overview: DashboardOverview::default(),
                failed_resources: 0,
            });
            view.store.topic_current.replace(TopicCurrentLoad::default());
            view.store.broker_current.replace(Vec::new());
            view.store.topic_history.replace(Vec::new());
            view.store.broker_history.replace(Vec::new());
            view.selected_topic = Some("orders".into());
            view.selected_broker = Some(BrokerIdentity {
                cluster: "a".into(),
                broker_name: "broker".into(),
                broker_id: 0,
                address: "first:10911".into(),
            });
            let old_request = view.store.topic_current.begin(10).unwrap();
            view.topic_picker.update(cx, |picker, cx| {
                picker.set_items(
                    SearchableVec::new(vec![
                        TopicHistoryOption(None),
                        TopicHistoryOption(Some("orders".into())),
                    ]),
                    window,
                    cx,
                );
                picker.set_selected_value(&Some("orders".into()), window, cx);
            });
            view.clear_for_environment(11, window, cx);
            assert_eq!(view.topic_picker.read(cx).selected_value(), Some(&None));
            assert!(view.store.overview.state.value().is_none());
            assert!(view.store.topic_current.state.value().is_none());
            assert!(view.store.broker_current.state.value().is_none());
            assert!(view.store.topic_history.state.value().is_none());
            assert!(view.store.broker_history.state.value().is_none());
            assert!(view.selected_topic.is_none());
            assert!(view.selected_broker.is_none());
            assert!(
                !view
                    .store
                    .topic_current
                    .finish(old_request, 11, Ok(Some(TopicCurrentLoad::default())))
            );
            assert!(matches!(view.store.topic_current.state, Loadable::Idle));
        })
    });
}

#[gpui::test]
fn retained_dashboard_resources_render_refresh_and_inline_error_retry_states(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    let services =
        AppServices::default().with_dashboard_broker_operations(Arc::new(FakeDashboardBrokerOperations::default()));
    let capture = Rc::new(RefCell::new(None));
    let capture_view = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let view = cx.new(|cx| DashboardView::new(window, services, 11, cx));
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
                Ok(Some(load(vec![TopicCurrentObservation {
                    topic: "orders".into(),
                    total_messages: Observed::Observed(7),
                    produce_tps: Observed::Unknown,
                    consume_tps: Observed::Unknown,
                }])))
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

fn load(items: Vec<TopicCurrentObservation>) -> TopicCurrentLoad {
    TopicCurrentLoad {
        total_topics: items.len(),
        application_topics: items.len(),
        candidates: items.iter().map(|item| item.topic.clone()).collect(),
        available_topics: items.iter().map(|item| item.topic.clone()).collect(),
        items,
        ..Default::default()
    }
}

fn click(cx: &mut gpui::VisualTestContext, selector: &'static str) {
    let bounds = cx
        .debug_bounds(selector)
        .unwrap_or_else(|| panic!("missing {selector}"));
    cx.simulate_click(bounds.center(), gpui::Modifiers::default());
    cx.run_until_parked();
}

#[gpui::test]
fn utc_day_topic_and_broker_metric_controls_issue_the_selected_history_queries(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    let fake = Arc::new(FakeDashboardBrokerOperations::default());
    fake.queue_topic_current(Ok(vec![TopicCurrentObservation {
        topic: "orders".into(),
        total_messages: Observed::Observed(2),
        produce_tps: Observed::Observed(0.5),
        consume_tps: Observed::Observed(0.0),
    }]));
    fake.queue_broker_current(Ok(vec![BrokerCurrentMetric::observed(
        BrokerIdentity {
            cluster: "c".into(),
            broker_name: "b".into(),
            broker_id: 0,
            address: "b:10911".into(),
        },
        "5.3.2".into(),
        0.5,
        0.0,
    )]));
    let capture = Rc::new(RefCell::new(None));
    let capturing = capture.clone();
    let services = AppServices::default().with_dashboard_broker_operations(fake.clone());
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let view = cx.new(|cx| DashboardView::new(window, services, 9, cx));
        capturing.replace(Some(view.clone()));
        Root::new(view, window, cx)
    });
    let view = capture.borrow_mut().take().unwrap();
    cx.run_until_parked();
    cx.simulate_resize(size(px(1440.), px(1400.)));
    cx.draw(point(px(0.), px(0.)), size(px(1440.), px(1400.)), |_, _| root.clone());
    let initial = fake.calls();
    assert!(initial.topic_history.is_empty());
    assert!(
        initial
            .count_history
            .iter()
            .any(|(metric, _, _)| *metric == HistoryMetricKind::TopicCount)
    );
    assert!(
        initial
            .count_history
            .iter()
            .any(|(metric, _, _)| *metric == HistoryMetricKind::BrokerCount)
    );
    let original_day = cx.read(|app| view.read(app).history_date);
    click(cx, "dashboard-history-previous-day");
    assert_eq!(
        cx.read(|app| view.read(app).history_date),
        original_day.pred_opt().unwrap()
    );
    let day = cx.read(|app| view.read(app).history_date);
    let expected = utc_day_range(day).unwrap();
    assert_eq!(expected.1 - expected.0, 86_399_999);
    assert!(
        fake.calls()
            .count_history
            .iter()
            .rev()
            .take(2)
            .all(|(_, start, end)| (*start, *end) == expected)
    );
    cx.draw(point(px(0.), px(0.)), size(px(1440.), px(1400.)), |_, _| root.clone());
    fake.queue_topic_history(Ok(vec![HistoryPoint {
        metric: HistoryMetricKind::TopicMessages,
        series_identity: "orders".into(),
        timestamp_epoch_ms: expected.0 + 1_000,
        value: 2.0,
        source_revision: 9,
    }]));
    click(cx, "dashboard-history-topic");
    cx.simulate_keystrokes("down enter");
    cx.run_until_parked();
    assert_eq!(
        cx.read(|app| view.read(app).selected_topic.clone()),
        Some("orders".into())
    );
    assert_eq!(
        fake.calls().topic_history.last().unwrap(),
        &("orders".into(), expected.0, expected.1)
    );
    cx.draw(point(px(0.), px(0.)), size(px(1440.), px(1400.)), |_, _| root.clone());
    fake.queue_broker_history(Ok(vec![HistoryPoint {
        metric: HistoryMetricKind::BrokerConsumeTps,
        series_identity: "broker".into(),
        timestamp_epoch_ms: expected.0 + 1_000,
        value: 0.5,
        source_revision: 9,
    }]));
    click(cx, "dashboard-history-broker-consume");
    assert_eq!(
        fake.calls().broker_history.last().unwrap().0,
        HistoryMetricKind::BrokerConsumeTps
    );
    assert_eq!(fake.calls().broker_history.last().unwrap().2, expected.0);
    assert_eq!(fake.calls().overview_revisions, [9]);
    assert_eq!(fake.calls().topic_current_revisions, [9]);
    let last_success = cx
        .read(|app| view.read(app).store.topic_history.last_updated_epoch_ms())
        .unwrap();
    fake.queue_topic_history(Err(error()));
    cx.draw(point(px(0.), px(0.)), size(px(1440.), px(1400.)), |_, _| root.clone());
    click(cx, "retry-topic-history");
    cx.read(|app| {
        let view = view.read(app);
        assert!(matches!(
            view.store.topic_history.state,
            Loadable::Failed { previous: Some(_), .. }
        ));
        assert_eq!(view.store.topic_history.state.value().unwrap()[0].value, 2.0);
        assert_eq!(view.store.topic_history.last_updated_epoch_ms(), Some(last_success));
    });

    // Returning to All topics uses the count metric; it does not sum the twenty sampled Topics.
    cx.draw(point(px(0.), px(0.)), size(px(1440.), px(1400.)), |_, _| root.clone());
    click(cx, "dashboard-history-topic");
    cx.simulate_keystrokes("up enter");
    cx.run_until_parked();
    assert!(cx.read(|app| view.read(app).selected_topic.is_none()));
    assert_eq!(
        fake.calls().count_history.last().unwrap().0,
        HistoryMetricKind::TopicCount
    );
    // A catalog refresh that removes the applied Topic must also select All topics in the widget.
    cx.draw(point(px(0.), px(0.)), size(px(1440.), px(1400.)), |_, _| root.clone());
    click(cx, "dashboard-history-topic");
    cx.simulate_keystrokes("down enter");
    cx.run_until_parked();
    assert_eq!(
        cx.read(|app| view.read(app).selected_topic.clone()),
        Some("orders".into())
    );
    fake.queue_topic_current(Ok(Vec::new()));
    cx.update(|_, app| view.update(app, |view, cx| view.refresh_topic_current(cx)));
    cx.run_until_parked();
    cx.read(|app| {
        let view = view.read(app);
        assert!(view.selected_topic.is_none());
        assert_eq!(view.topic_picker.read(app).selected_value(), Some(&None));
    });
}

#[test]
fn utc_day_ranges_include_the_final_millisecond_and_exclude_the_next_day() {
    let day = chrono::NaiveDate::from_ymd_opt(2026, 10, 29).unwrap();
    let (start, end) = utc_day_range(day).unwrap();
    let next = utc_day_range(day.succ_opt().unwrap()).unwrap().0;
    let points = [start - 1, start, end, next].map(|timestamp_epoch_ms| HistoryPoint {
        metric: HistoryMetricKind::TopicCount,
        series_identity: "cluster".into(),
        timestamp_epoch_ms,
        value: 0.0,
        source_revision: 1,
    });
    let filtered = rocketmq_dashboard_common::filter_history_range(&points, start, end);
    assert_eq!(
        filtered
            .iter()
            .map(|point| point.timestamp_epoch_ms)
            .collect::<Vec<_>>(),
        [start, end]
    );
}

#[gpui::test]
fn history_cards_share_width_and_separate_exact_point_labels_at_compact_and_fullscreen_sizes(
    cx: &mut gpui::TestAppContext,
) {
    cx.update(gpui_component::init);
    let services =
        AppServices::default().with_dashboard_broker_operations(Arc::new(FakeDashboardBrokerOperations::default()));
    let capture = Rc::new(RefCell::new(None));
    let capturing = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let view = cx.new(|cx| DashboardView::new(window, services, 9, cx));
        capturing.replace(Some(view.clone()));
        Root::new(view, window, cx)
    });
    let view = capture.borrow_mut().take().unwrap();
    cx.run_until_parked();
    cx.update(|_, app| {
        view.update(app, |view, cx| {
            view.selected_topic = Some("orders".into());
            view.store.topic_history.replace(vec![HistoryPoint {
                metric: HistoryMetricKind::TopicMessages,
                series_identity: "orders".into(),
                timestamp_epoch_ms: 1_793_232_000_000,
                value: 12_345_678_901.0,
                source_revision: 9,
            }]);
            cx.notify();
        });
    });

    for metric in [HistoryMetricKind::BrokerCount, HistoryMetricKind::BrokerConsumeTps] {
        cx.update(|_, app| {
            view.update(app, |view, cx| {
                view.broker_metric = metric;
                view.store.broker_history.replace(vec![HistoryPoint {
                    metric,
                    series_identity: "broker".into(),
                    timestamp_epoch_ms: 1_793_232_000_000,
                    value: if metric == HistoryMetricKind::BrokerCount {
                        1.0
                    } else {
                        0.0
                    },
                    source_revision: 9,
                }]);
                cx.notify();
            });
        });
        for width in [960., 1_440., 2_560.] {
            let viewport = size(px(width), px(1_600.));
            cx.simulate_resize(viewport);
            cx.draw(point(px(0.), px(0.)), viewport, |_, _| root.clone());
            let topic = cx.debug_bounds("dashboard-topic-history-card").unwrap();
            let broker = cx.debug_bounds("dashboard-broker-history-card").unwrap();
            assert!(
                (f32::from(topic.size.width) - f32::from(broker.size.width)).abs() < 1.,
                "unequal history cards at {width}px: {topic:?}, {broker:?}"
            );
            assert!(topic.right() < broker.left());
            assert_eq!(topic.top(), broker.top());
            assert!(topic.left() >= px(0.) && broker.right() <= px(width));
            for (series, card, chart_selector, time_selector, value_selector) in [
                (
                    "topic",
                    topic,
                    "dashboard-topic-history-chart",
                    "dashboard-topic-history-time-0",
                    "dashboard-topic-history-value-0",
                ),
                (
                    "broker",
                    broker,
                    "dashboard-broker-history-chart",
                    "dashboard-broker-history-time-0",
                    "dashboard-broker-history-value-0",
                ),
            ] {
                let chart = cx.debug_bounds(chart_selector).unwrap();
                let time = cx.debug_bounds(time_selector).unwrap();
                let value = cx.debug_bounds(value_selector).unwrap();
                assert!(chart.size.width >= card.size.width - px(1.));
                assert!(chart.size.width > px(width / 3.), "narrow {series} chart at {width}px");
                assert!(time.size.width > px(120.), "timestamp clipped at {width}px");
                assert!(value.size.width > px(20.), "value clipped at {width}px");
                assert!(time.left() >= card.left() && time.right() <= card.right());
                assert!(value.left() >= card.left() && value.right() <= card.right());
                assert!(
                    value.left() >= time.right() + px(12.) || value.top() > time.bottom(),
                    "point labels run together for {series} at {width}px: {time:?}, {value:?}"
                );
            }
        }
    }
}
