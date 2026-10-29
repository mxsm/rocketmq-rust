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

use std::sync::Arc;

use super::BrokersView;
use crate::{
    services::{AppServices, dashboard_broker::test_support::FakeDashboardBrokerOperations},
    state::{Loadable, UiError, UiErrorCode},
};

#[gpui::test]
fn audit_ordinary_broker_sheet_close_drops_the_view_waiter_but_commits_the_owned_terminal(
    cx: &mut gpui::TestAppContext,
) {
    use crate::{
        infrastructure::{
            auth_state::{DesktopAuthState, MapEnvironment},
            config_store::DesktopConfigStore,
        },
        route::BrokerTab,
        services::brokers::BrokerConfigMutationResult,
    };
    use gpui::{
        AppContext as _, Context, Entity, IntoElement, ParentElement as _, Render, Styled as _, Window, div, point, px,
        size,
    };
    use gpui_component::Root;
    use rocketmq_dashboard_common::{
        AuditAction, AuditFilter, AuditOutcome, AuditQuery, BrokerConfigSnapshot, BrokerIdentity, BrokerInventoryItem,
        BrokerRole, EndpointAvailability, Observed,
    };
    use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};
    use std::{cell::RefCell, collections::BTreeMap, rc::Rc, time::Duration};

    struct Harness(Entity<BrokersView>);
    impl Render for Harness {
        fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
            div()
                .size_full()
                .child(self.0.clone())
                .children(Root::render_sheet_layer(window, cx))
                .children(Root::render_dialog_layer(window, cx))
        }
    }

    cx.update(gpui_component::init);
    let directory = tempfile::tempdir().unwrap();
    let runtime = RuntimeOwner::plan(RuntimeConfig::for_parallelism("audit-sheet-close", 1))
        .unwrap()
        .with_memory_limit(ProcessMemoryLimit::configured(256 * 1024 * 1024).unwrap())
        .build()
        .unwrap();
    let mutations = runtime.root_context().component("mutations");
    let store = DesktopConfigStore::new(
        directory.path().join("config.json"),
        runtime.root_context().component("storage"),
    );
    let fake = Arc::new(FakeDashboardBrokerOperations::default());
    let identity = BrokerIdentity {
        cluster: "a".into(),
        broker_name: "broker".into(),
        broker_id: 0,
        address: "first:10911".into(),
    };
    let item = BrokerInventoryItem {
        identity: identity.clone(),
        role: BrokerRole::Master,
        version: Observed::Unknown,
        availability: EndpointAvailability::Available,
        produce_tps: Observed::Unknown,
        consume_tps: Observed::Unknown,
    };
    fake.queue_inventory(Ok(vec![item.clone()]));
    fake.queue_inventory(Ok(vec![item.clone()]));
    fake.queue_config(Ok(BrokerConfigSnapshot::new(
        identity.clone(),
        4,
        BTreeMap::from([("flushDiskType".into(), "ASYNC_FLUSH".into())]),
    )));
    fake.queue_patch(Ok(BrokerConfigMutationResult::AppliedReloadFailed {
        previous_generation: 4,
        reported_generation: 5,
        invalidations: Vec::new(),
        error: UiError::new("Refresh failed.", UiErrorCode::Connection, true),
    }));
    let (started, release) = fake.hold_next_patch();
    let services = AppServices::desktop_with_fake_provider(
        store,
        DesktopAuthState::new(Arc::new(MapEnvironment::new([]))),
        mutations.clone(),
        runtime.root_context().component("history"),
        runtime.root_context().component("monitor"),
    )
    .with_dashboard_broker_operations(fake.clone());
    runtime.block_on(services.bootstrap()).unwrap();
    let captured = Rc::new(RefCell::new(None));
    let capture = captured.clone();
    let view_services = services.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let view = cx.new(|cx| BrokersView::new(window, view_services, 0, cx));
        capture.replace(Some(view.clone()));
        let harness = cx.new(|_| Harness(view));
        Root::new(harness, window, cx)
    });
    let view = captured.borrow_mut().take().unwrap();
    cx.run_until_parked();
    cx.update(|window, app| {
        view.update(app, |view, cx| {
            view.open_identity(item, BrokerTab::Configuration, false, window, cx)
        })
    });
    cx.run_until_parked();
    let inspector = cx.read(|app| view.read(app).inspector.clone().unwrap());
    let weak_inspector = inspector.downgrade();
    cx.update(|window, app| {
        inspector.update(app, |inspector, cx| {
            inspector.submit_value_for_test("flushDiskType", "SYNC_FLUSH".into(), window, cx)
        })
    });
    cx.run_until_parked();
    runtime.block_on(started).unwrap();
    drop(inspector);
    cx.draw(point(px(0.), px(0.)), size(px(1440.), px(900.)), |_, _| root.clone());
    cx.simulate_keystrokes("escape");
    cx.draw(point(px(0.), px(0.)), size(px(1440.), px(900.)), |_, _| root.clone());
    cx.run_until_parked();
    cx.read(|app| {
        assert!(view.read(app).inspector.is_none());
        assert!(
            view.read(app).pending_inspectors.is_empty(),
            "ordinary close must not retain hidden view owners"
        );
        assert!(weak_inspector.upgrade().is_none());
    });
    release.send(()).unwrap();
    assert!(
        runtime
            .block_on(mutations.task_group().shutdown(Duration::from_secs(5)))
            .is_healthy()
    );
    let page = services
        .query_audit(AuditQuery {
            filter: AuditFilter::recent(crate::services::audit::now_epoch_ms().unwrap()),
            limit: 20,
            cursor: None,
        })
        .unwrap();
    assert_eq!(page.events.len(), 1);
    assert_eq!(page.events[0].operation.action, AuditAction::BrokerUpdated);
    assert_eq!(page.events[0].outcome, AuditOutcome::Succeeded);
    assert!(page.events[0].facts.refresh_failed);
    assert_eq!(fake.calls().patches.len(), 1);
    runtime.shutdown_runtime_blocking().unwrap();
}

#[gpui::test]
fn environment_clear_removes_broker_selection_but_retains_an_accepted_write(cx: &mut gpui::TestAppContext) {
    use super::BrokerInspector;
    use crate::route::BrokerTab;
    use gpui::AppContext as _;
    use rocketmq_dashboard_common::{BrokerIdentity, BrokerRole, EndpointAvailability, Observed};

    cx.update(gpui_component::init);
    let (view, cx) = cx.add_window_view(|window, cx| BrokersView::new(window, AppServices::default(), 10, cx));
    cx.run_until_parked();
    let (complete, completion) = tokio::sync::oneshot::channel::<()>();
    let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let finished_task = finished.clone();
    cx.update(|window, app| {
        view.update(app, |view, cx| {
            let identity = BrokerIdentity {
                cluster: "a".into(),
                broker_name: "broker".into(),
                broker_id: 0,
                address: "first:10911".into(),
            };
            view.store
                .inventory
                .replace(vec![rocketmq_dashboard_common::BrokerInventoryItem {
                    identity: identity.clone(),
                    role: BrokerRole::Master,
                    version: Observed::Unknown,
                    availability: EndpointAvailability::Available,
                    produce_tps: Observed::Unknown,
                    consume_tps: Observed::Unknown,
                }]);
            view.store.select(identity.clone(), BrokerTab::Overview).unwrap();
            let inspector = cx
                .new(|cx| BrokerInspector::new(window, AppServices::default(), 10, identity, BrokerTab::Overview, cx));
            inspector.update(cx, |inspector, cx| {
                inspector.set_mutation_task_for_test(Some(cx.spawn(async move |owner, cx| {
                    completion.await.unwrap();
                    finished_task.store(true, std::sync::atomic::Ordering::SeqCst);
                    let _ = owner.update(cx, |inspector, cx| {
                        inspector.set_mutation_task_for_test(None);
                        cx.notify();
                    });
                })));
            });
            view.inspector = Some(inspector);
            view.clear_for_environment(11, window, cx);
            assert!(view.store.inventory.state.value().is_none());
            assert!(view.store.selected.is_none());
            assert!(view.inspector.is_none());
            assert_eq!(view.pending_inspectors.len(), 1);
        })
    });
    complete.send(()).unwrap();
    cx.run_until_parked();
    assert!(finished.load(std::sync::atomic::Ordering::SeqCst));
    cx.update(|_, app| assert!(view.read(app).pending_inspectors.is_empty()));
}

#[gpui::test]
fn initial_inventory_failure_remains_error_instead_of_becoming_empty(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    let fake = Arc::new(FakeDashboardBrokerOperations::default());
    fake.queue_inventory(Err(UiError::new(
        "Broker inventory unavailable",
        UiErrorCode::Connection,
        true,
    )));
    let services = AppServices::default().with_dashboard_broker_operations(fake.clone());
    let (view, cx) = cx.add_window_view(move |window, cx| BrokersView::new(window, services, 4, cx));
    cx.run_until_parked();

    cx.read(|app| {
        let view = view.read(app);
        assert!(matches!(
            &view.store.inventory.state,
            Loadable::Failed { previous: None, error } if error.summary() == "Broker inventory unavailable"
        ));
        assert!(!matches!(view.store.inventory.state, Loadable::Empty));
        assert!(view.store.visible_page().is_empty());
        assert_eq!(view._subscriptions.len(), 2);
    });
    assert_eq!(fake.calls().inventory_revisions, [4]);
}
