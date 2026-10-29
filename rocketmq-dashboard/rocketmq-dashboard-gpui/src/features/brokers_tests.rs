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
