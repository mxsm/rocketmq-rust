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

use super::*;
use crate::services::dlq::test_support::{FakeDlqBackend, attempt, csv, group_inventory, page, row};
use gpui::{Modifiers, point, size};
use gpui_component::Root;
use rocketmq_dashboard_common::{DlqEntryState, DlqPage};
use std::{cell::RefCell, rc::Rc, sync::Arc};
struct Harness {
    dlq: Entity<DlqView>,
}
impl Render for Harness {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .child(self.dlq.clone())
            .children(Root::render_sheet_layer(window, cx))
            .children(Root::render_dialog_layer(window, cx))
    }
}
fn open(
    cx: &mut gpui::TestAppContext,
    services: AppServices,
) -> (Entity<Root>, Entity<DlqView>, &mut gpui::VisualTestContext) {
    cx.update(gpui_component::init);
    let capture = Rc::new(RefCell::new(None));
    let captured = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let dlq = cx.new(|cx| DlqView::new(window, services, 0, cx));
        captured.replace(Some(dlq.clone()));
        let harness = cx.new(|_| Harness { dlq });
        Root::new(harness, window, cx)
    });
    let dlq = capture.borrow_mut().take().unwrap();
    cx.simulate_resize(size(px(1440.), px(900.)));
    (root, dlq, cx)
}
fn draw(root: &Entity<Root>, cx: &mut gpui::VisualTestContext) {
    cx.run_until_parked();
    for _ in 0..2 {
        cx.draw(point(px(0.), px(0.)), size(px(1440.), px(900.)), |_, _| root.clone());
    }
}
fn click(cx: &mut gpui::VisualTestContext, id: &'static str) {
    let bounds = cx.debug_bounds(id).unwrap_or_else(|| panic!("missing control {id}"));
    cx.simulate_click(bounds.center(), Modifiers::default());
    cx.run_until_parked();
}
fn set_filters(view: &Entity<DlqView>, cx: &mut gpui::VisualTestContext) {
    cx.update(|window, app| {
        view.update(app, |view, cx| {
            for (input, value) in [
                (&view.group, "group-a"),
                (&view.begin, "1970-01-01 00:00:00"),
                (&view.end, "1970-01-01 00:00:01"),
            ] {
                input.update(cx, |input, cx| input.set_value(value, window, cx));
            }
        })
    });
    cx.run_until_parked();
}
#[gpui::test]
fn dlq_controls_apply_server_pages_exact_id_refresh_and_clear_selection(cx: &mut gpui::TestAppContext) {
    let backend = Arc::new(FakeDlqBackend::default());
    let (root, view, cx) = open(cx, AppServices::default().with_dlq_backend(backend.clone()));
    set_filters(&view, cx);
    backend.queries.lock().unwrap().push_back(Ok(page()));
    draw(&root, cx);
    click(cx, "dlq-search");
    draw(&root, cx);
    click(cx, "dlq-select-0");
    assert_eq!(cx.read(|app| view.read(app).store.selected.len()), 1);
    backend.queries.lock().unwrap().push_back(Ok(DlqPage {
        rows: Vec::new(),
        extent: MessagePageExtent::End,
    }));
    draw(&root, cx);
    click(cx, "dlq-next");
    assert_eq!(backend.calls().queries[1].page, 2);
    assert!(cx.read(|app| view.read(app).store.selected.is_empty()));
    backend.queries.lock().unwrap().push_back(Ok(page()));
    draw(&root, cx);
    click(cx, "dlq-previous");
    assert_eq!(backend.calls().queries[2].page, 1);
    cx.update(|window, app| {
        view.update(app, |view, cx| {
            view.message_id
                .update(cx, |input, cx| input.set_value("exact-id", window, cx))
        })
    });
    cx.run_until_parked();
    assert!(cx.read(|app| matches!(view.read(app).store.rows, Loadable::Idle)));
    backend.queries.lock().unwrap().push_back(Ok(page()));
    draw(&root, cx);
    click(cx, "dlq-refresh");
    assert!(backend.calls().queries[3].message_id.is_none());
    backend.queries.lock().unwrap().push_back(Ok(page()));
    draw(&root, cx);
    click(cx, "dlq-search");
    let call = backend.calls().queries.last().unwrap().clone();
    assert_eq!(call.message_id.as_deref(), Some("exact-id"));
    assert_eq!((call.begin, call.end, call.page, call.page_size), (0, 1000, 1, 20));
    for (width, height) in [(930., 620.), (960., 640.)] {
        let viewport = size(px(width), px(height));
        cx.simulate_resize(viewport);
        for _ in 0..2 {
            cx.draw(point(px(0.), px(0.)), viewport, |_, _| root.clone());
        }
        let field = cx.debug_bounds("dlq-id-field").unwrap();
        assert!(
            field.size.width >= px(240.) && field.size.height >= px(28.),
            "collapsed DLQ ID field at {width}x{height}: {field:?}"
        );
        assert!(cx.debug_bounds("dlq-search").unwrap().bottom() <= px(height));
    }
}
#[gpui::test]
fn dlq_real_confirmation_cancel_double_click_partial_results_and_failed_only_retry(cx: &mut gpui::TestAppContext) {
    let backend = Arc::new(FakeDlqBackend::default());
    let (root, view, cx) = open(cx, AppServices::default().with_dlq_backend(backend.clone()));
    set_filters(&view, cx);
    let mut initial = page();
    let mut invalid = row(3);
    invalid.target = Err(rocketmq_dashboard_common::DlqIdentityError::MissingTopic);
    initial.rows.push(invalid);
    backend.queries.lock().unwrap().push_back(Ok(initial));
    draw(&root, cx);
    click(cx, "dlq-search");
    draw(&root, cx);
    click(cx, "dlq-select-2");
    assert!(cx.read(|app| view.read(app).store.selected.is_empty()));
    click(cx, "dlq-select-0");
    draw(&root, cx);
    click(cx, "dlq-select-1");
    draw(&root, cx);
    click(cx, "dlq-recover");
    draw(&root, cx);
    click(cx, "dlq-confirm-cancel");
    assert!(backend.calls().consumes.is_empty());
    backend.consumes.lock().unwrap().extend([
        attempt(DlqEntryState::Succeeded),
        attempt(DlqEntryState::BusinessFailure),
    ]);
    let (mut started, release, _) = backend.hold_consume();
    draw(&root, cx);
    click(cx, "dlq-recover");
    draw(&root, cx);
    click(cx, "dlq-confirm-submit");
    assert!(started.try_recv().is_ok());
    draw(&root, cx);
    click(cx, "dlq-confirm-submit");
    assert_eq!(backend.calls().consumes.len(), 1);
    click(cx, "dlq-confirm-cancel");
    release.send(()).unwrap();
    draw(&root, cx);
    assert_eq!(backend.calls().consumes.len(), 2);
    cx.read(|app| {
        let view = view.read(app);
        assert!(view.store.selected.is_empty());
        assert_eq!(
            view.store.results.as_ref().unwrap().retryable_targets(),
            vec![row(2).target.unwrap()]
        );
    });
    backend
        .consumes
        .lock()
        .unwrap()
        .push_back(attempt(DlqEntryState::Succeeded));
    click(cx, "dlq-retry-failed");
    draw(&root, cx);
    assert_eq!(
        cx.read(|app| view.read(app).confirmation.as_ref().unwrap().read(app).targets.clone()),
        vec![row(2).target.unwrap()]
    );
    click(cx, "dlq-confirm-submit");
    draw(&root, cx);
    assert_eq!(backend.calls().consumes.len(), 3);
    assert_eq!(backend.calls().consumes[2].1, row(2).target.unwrap());
    assert!(cx.read(|app| {
        view.read(app)
            .store
            .results
            .as_ref()
            .unwrap()
            .retryable_targets()
            .is_empty()
    }));
}
#[gpui::test]
fn dlq_discovered_group_and_bounded_inspector_clear_on_context_change(cx: &mut gpui::TestAppContext) {
    use crate::services::{consumers::test_support::FakeConsumerBackend, messages::test_support::FakeMessageBackend};
    use rocketmq_dashboard_common::MessagePayload;
    let backend = Arc::new(FakeDlqBackend::default());
    let consumers = Arc::new(FakeConsumerBackend::default());
    let messages = Arc::new(FakeMessageBackend::default());
    let (root, view, cx) = open(
        cx,
        AppServices::default()
            .with_dlq_backend(backend.clone())
            .with_consumer_backend(consumers.clone())
            .with_message_backend(messages.clone()),
    );
    consumers.queue_inventory(Ok(group_inventory()));
    draw(&root, cx);
    click(cx, "dlq-groups-refresh");
    draw(&root, cx);
    click(cx, "dlq-group-picker");
    cx.simulate_keystrokes("down enter");
    cx.run_until_parked();
    assert_eq!(
        cx.read(|app| view.read(app).group.read(app).value().to_string()),
        "group-a"
    );
    assert_eq!(
        consumers.calls().inventory[0].query_scope,
        rocketmq_dashboard_common::ConsumerQueryScope::Configured
    );
    assert!(backend.calls().queries.is_empty());
    set_filters(&view, cx);
    backend.queries.lock().unwrap().push_back(Ok(page()));
    draw(&root, cx);
    click(cx, "dlq-search");
    draw(&root, cx);
    click(cx, "dlq-open-0");
    draw(&root, cx);
    assert!(cx.debug_bounds("message-consume").is_some());
    click(cx, "message-consume");
    assert!(messages.calls().consume.is_empty() && backend.calls().consumes.is_empty());
    messages.queue_payload(Ok(MessagePayload::bounded(
        row(1).metadata.identity,
        b"private-body".to_vec(),
        Default::default(),
    )));
    click(cx, "message-tab-body");
    draw(&root, cx);
    let detail = cx.read(|app| view.read(app).detail.clone().unwrap());
    assert!(cx.read(|app| detail.read(app).has_payload()));
    cx.update(|window, app| view.update(app, |view, cx| view.set_revision(1, window, cx)));
    cx.run_until_parked();
    assert!(!cx.read(|app| detail.read(app).has_payload()));
    assert!(cx.read(|app| view.read(app).group.read(app).value().is_empty()));
    assert!(cx.read(|app| view.read(app).store.applied.is_none()));
}
#[gpui::test]
fn dlq_group_reload_clears_candidates_when_scope_changes_before_completion(cx: &mut gpui::TestAppContext) {
    use crate::{
        infrastructure::{
            auth_state::{DesktopAuthState, MapEnvironment},
            config_store::DesktopConfigStore,
        },
        services::{ConfigMutation, consumers::test_support::FakeConsumerBackend},
    };
    use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};
    use std::time::Duration;
    let directory = tempfile::tempdir().unwrap();
    let runtime = RuntimeOwner::plan(RuntimeConfig::for_parallelism("dlq-group-scope", 1))
        .unwrap()
        .with_memory_limit(ProcessMemoryLimit::configured(256 * 1024 * 1024).unwrap())
        .build()
        .unwrap();
    let consumers = Arc::new(FakeConsumerBackend::default());
    let services = AppServices::desktop_with_fake_provider(
        DesktopConfigStore::new(
            directory.path().join("config.json"),
            runtime.root_context().component("storage"),
        ),
        DesktopAuthState::new(Arc::new(MapEnvironment::new([]))),
        runtime.root_context().component("application"),
        runtime.root_context().component("history"),
        runtime.root_context().component("monitor"),
    )
    .with_consumer_backend(consumers.clone());
    runtime.block_on(services.bootstrap()).unwrap();
    let (completion, completed) = std::sync::mpsc::channel();
    let (root, view, cx) = open(cx, services.clone().with_runtime_completion(completion));
    consumers.queue_inventory(Ok(group_inventory()));
    cx.update(|window, app| view.update(app, |view, cx| view.ensure_groups(window, cx)));
    cx.run_until_parked();
    assert_eq!(
        completed.recv_timeout(Duration::from_secs(5)).unwrap(),
        "gpui-service-consumer-inventory"
    );
    draw(&root, cx);
    assert_eq!(
        cx.read(|app| view.read(app).groups.state.value().unwrap().groups.len()),
        1
    );
    let release = consumers.hold_next_inventory();
    let started = consumers.observe_next_inventory();
    consumers.queue_inventory(Ok(group_inventory()));
    click(cx, "dlq-groups-refresh");
    runtime.block_on(started).unwrap();
    runtime.block_on(async {
        let (progress, _) = tokio::sync::mpsc::unbounded_channel();
        services
            .mutate_with_progress(ConfigMutation::AddProxy("proxy:8080".into()), progress.clone())
            .await
            .unwrap();
        services
            .mutate_with_progress(ConfigMutation::SwitchProxy("proxy:8080".into()), progress)
            .await
            .unwrap();
    });
    release.send(()).unwrap();
    assert_eq!(
        completed.recv_timeout(Duration::from_secs(5)).unwrap(),
        "gpui-service-consumer-inventory"
    );
    draw(&root, cx);
    cx.read(|app| match &view.read(app).groups.state {
        Loadable::Failed { previous: None, error } => {
            assert_eq!(error.code(), UiErrorCode::CapabilityUnavailable);
            assert!(error.summary().contains("NameServer Direct"));
        }
        state => panic!("stale Group candidates survived the scope change: {state:?}"),
    });
    cx.update(|window, app| view.update(app, |view, cx| view.ensure_groups(window, cx)));
    draw(&root, cx);
    click(cx, "dlq-groups-refresh");
    assert_eq!(consumers.calls().inventory.len(), 2);
    assert!(runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}
#[gpui::test]
fn dlq_native_save_cancel_writes_nothing_and_real_write_failure_retains_applied_page(cx: &mut gpui::TestAppContext) {
    use crate::infrastructure::{
        auth_state::{DesktopAuthState, MapEnvironment},
        config_store::DesktopConfigStore,
    };
    use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};
    use std::time::Duration;
    let directory = tempfile::tempdir().unwrap();
    let runtime = RuntimeOwner::plan(RuntimeConfig::for_parallelism("dlq-native-export", 1))
        .unwrap()
        .with_memory_limit(ProcessMemoryLimit::configured(256 * 1024 * 1024).unwrap())
        .build()
        .unwrap();
    let backend = Arc::new(FakeDlqBackend::default());
    let store = DesktopConfigStore::new(
        directory.path().join("config.json"),
        runtime.root_context().component("storage"),
    );
    let services = AppServices::desktop_with_fake_provider(
        store,
        DesktopAuthState::new(Arc::new(MapEnvironment::new([]))),
        runtime.root_context().component("application"),
        runtime.root_context().component("history"),
        runtime.root_context().component("monitor"),
    )
    .with_dlq_backend(backend.clone());
    runtime.block_on(services.bootstrap()).unwrap();
    let (completion, completed) = std::sync::mpsc::channel();
    let services = services.with_runtime_completion(completion);
    let (root, view, cx) = open(cx, services);
    set_filters(&view, cx);
    backend.queries.lock().unwrap().push_back(Ok(page()));
    draw(&root, cx);
    click(cx, "dlq-search");
    assert_eq!(
        completed.recv_timeout(Duration::from_secs(5)).unwrap(),
        "gpui-service-dlq-query"
    );
    draw(&root, cx);
    click(cx, "dlq-export");
    assert!(cx.did_prompt_for_new_path());
    cx.simulate_new_path_selection(|_| None);
    draw(&root, cx);
    assert!(backend.calls().exports.is_empty());
    backend
        .exports
        .lock()
        .unwrap()
        .push_back(Ok(csv("private CSV body".into())));
    click(cx, "dlq-export");
    cx.simulate_new_path_selection(|_| Some(directory.path().join("missing-parent").join("page.csv")));
    cx.run_until_parked();
    assert_eq!(
        completed.recv_timeout(Duration::from_secs(5)).unwrap(),
        "gpui-service-dlq-export"
    );
    draw(&root, cx);
    cx.read(|app| {
        let view = view.read(app);
        assert_eq!(view.store.applied.as_ref().unwrap().consumer_group, "group-a");
        assert_eq!(view.store.rows.value().unwrap().rows.len(), 2);
        assert!(view.status.as_ref().unwrap().contains("Unable to write"));
    });
    assert_eq!(backend.calls().exports.len(), 1);
    assert_eq!(backend.calls().queries.len(), 1);
    assert!(runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}
