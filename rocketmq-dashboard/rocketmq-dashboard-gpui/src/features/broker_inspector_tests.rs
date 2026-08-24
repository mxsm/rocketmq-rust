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

use std::{cell::RefCell, collections::BTreeMap, rc::Rc, sync::Arc};

use gpui::{
    AppContext as _, Context, Entity, IntoElement, Modifiers, ParentElement as _, Render, Styled as _, Window, div,
    point, px, size,
};
use gpui_component::{Root, WindowExt as _};
use rocketmq_dashboard_common::{
    BrokerConfigSnapshot, BrokerIdentity, BrokerInventoryItem, BrokerRole, EndpointAvailability, Observed, RuntimeEntry,
};

use super::BrokerInspector;
use crate::{
    features::inspector_store::ConfigSubmissionState,
    route::BrokerTab,
    services::{AppServices, brokers::BrokerConfigMutationResult, delivery03::test_support::FakeDelivery03Backend},
    state::{Loadable, UiError, UiErrorCode},
};

struct DialogHarness {
    inspector: Entity<BrokerInspector>,
}

impl Render for DialogHarness {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let dialog_layer = Root::render_dialog_layer(window, cx);
        div().size_full().child(self.inspector.clone()).children(dialog_layer)
    }
}

fn identity() -> BrokerIdentity {
    BrokerIdentity {
        cluster: "cluster-a".into(),
        broker_name: "broker-a".into(),
        broker_id: 0,
        address: "127.0.0.1:10911".into(),
    }
}

fn inventory_item() -> BrokerInventoryItem {
    BrokerInventoryItem {
        identity: identity(),
        role: BrokerRole::Master,
        version: Observed::Observed("5.3.2".into()),
        availability: EndpointAvailability::Available,
        produce_tps: Observed::Observed(1.0),
        consume_tps: Observed::Observed(2.0),
    }
}

fn config(generation: u64, flush_disk_type: &str) -> BrokerConfigSnapshot {
    BrokerConfigSnapshot::new(
        identity(),
        generation,
        BTreeMap::from([
            ("flushDiskType".into(), flush_disk_type.into()),
            ("accessKey".into(), "must-not-cross-the-seam".into()),
        ]),
    )
}

fn config_entries(
    generation: u64,
    entries: impl IntoIterator<Item = (&'static str, &'static str)>,
) -> BrokerConfigSnapshot {
    BrokerConfigSnapshot::new(
        identity(),
        generation,
        entries
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect(),
    )
}

fn click_debug(cx: &mut gpui::VisualTestContext, selector: &'static str) {
    let bounds = cx
        .debug_bounds(selector)
        .unwrap_or_else(|| panic!("missing selector: {selector}"));
    cx.simulate_click(bounds.center(), Modifiers::default());
}

#[gpui::test]
fn tabs_are_lazy_and_runtime_and_config_refresh_locally(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    let fake = Arc::new(FakeDelivery03Backend::default());
    fake.queue_inventory(Ok(vec![inventory_item()]));
    fake.queue_runtime(Ok(vec![RuntimeEntry::new("brokerVersion".into(), "5.3.2".into())]));
    fake.queue_runtime(Ok(vec![RuntimeEntry::new("brokerVersion".into(), "5.3.3".into())]));
    fake.queue_config(Ok(config(7, "ASYNC_FLUSH")));
    let services = AppServices::default().with_delivery03_backend(fake.clone());
    let capture = Rc::new(RefCell::new(None));
    let capture_view = capture.clone();
    let (_root, cx) = cx.add_window_view(move |window, cx| {
        let inspector = cx.new(|cx| BrokerInspector::new(window, services, 5, identity(), BrokerTab::Overview, cx));
        capture_view.replace(Some(inspector.clone()));
        Root::new(inspector, window, cx)
    });
    let inspector = capture.borrow_mut().take().expect("inspector entity");
    cx.run_until_parked();
    assert_eq!(fake.calls().inventory_revisions, [5]);
    assert!(fake.calls().runtime.is_empty());
    assert!(fake.calls().config.is_empty());

    cx.update(|_, app| {
        inspector.update(app, |inspector, cx| {
            inspector.active_tab = BrokerTab::Runtime;
            inspector.refresh_active(cx);
        });
    });
    cx.run_until_parked();
    cx.read(|app| {
        let inspector = inspector.read(app);
        assert!(
            matches!(&inspector.store.runtime.state, Loadable::Ready(entries) if entries[0].display_value() == "5.3.2")
        );
    });
    assert_eq!(fake.calls().runtime.len(), 1);
    assert!(fake.calls().config.is_empty());

    cx.update(|_, app| inspector.update(app, |inspector, cx| inspector.refresh_runtime(cx)));
    cx.run_until_parked();
    cx.read(|app| {
        let inspector = inspector.read(app);
        assert!(
            matches!(&inspector.store.runtime.state, Loadable::Ready(entries) if entries[0].display_value() == "5.3.3")
        );
    });
    assert_eq!(fake.calls().runtime.len(), 2);
    assert_eq!(fake.calls().inventory_revisions, [5]);

    cx.update(|_, app| {
        inspector.update(app, |inspector, cx| {
            inspector.active_tab = BrokerTab::Configuration;
            inspector.refresh_active(cx);
        });
    });
    cx.run_until_parked();
    assert_eq!(fake.calls().config.len(), 1);
    assert_eq!(fake.calls().runtime.len(), 2);
}

#[gpui::test]
fn revision_change_blocks_writes_until_inventory_revalidation_and_authoritative_config_reload(
    cx: &mut gpui::TestAppContext,
) {
    cx.update(gpui_component::init);
    let fake = Arc::new(FakeDelivery03Backend::default());
    fake.queue_inventory(Ok(vec![inventory_item()]));
    fake.queue_config(Ok(config(7, "ASYNC_FLUSH")));
    let services = AppServices::default().with_delivery03_backend(fake.clone());
    let capture = Rc::new(RefCell::new(None));
    let capture_view = capture.clone();
    let (_root, cx) = cx.add_window_view(move |window, cx| {
        let inspector =
            cx.new(|cx| BrokerInspector::new(window, services, 1, identity(), BrokerTab::Configuration, cx));
        capture_view.replace(Some(inspector.clone()));
        Root::new(inspector, window, cx)
    });
    let inspector = capture.borrow_mut().take().expect("inspector entity");
    cx.run_until_parked();

    cx.update(|_, app| {
        inspector.update(app, |inspector, cx| {
            assert!(inspector.store.write_ready());
            inspector.set_revision(2, cx);
            assert!(inspector.store.stale);
            assert!(!inspector.store.write_ready());
            assert!(inspector.store.begin_submit(2).is_err());
            inspector.refresh_config(false, cx);
        });
    });
    cx.run_until_parked();
    assert_eq!(fake.calls().config.len(), 1, "stale target cannot load or write config");

    fake.queue_inventory(Ok(vec![inventory_item()]));
    fake.queue_config(Ok(config(8, "SYNC_FLUSH")));
    cx.update(|_, app| inspector.update(app, |inspector, cx| inspector.refresh_overview(cx)));
    cx.run_until_parked();
    cx.update(|_, app| {
        inspector.update(app, |inspector, cx| {
            assert!(inspector.store.is_validated_for(2));
            assert!(!inspector.store.write_ready());
            inspector.refresh_active(cx);
        });
    });
    cx.run_until_parked();
    cx.read(|app| {
        let inspector = inspector.read(app);
        assert!(inspector.store.write_ready());
        assert_eq!(
            inspector
                .store
                .config
                .state
                .value()
                .expect("reloaded config")
                .generation,
            8
        );
    });
    assert_eq!(fake.calls().inventory_revisions, [1, 2]);
    assert_eq!(
        fake.calls()
            .config
            .iter()
            .map(|(revision, _)| *revision)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert!(
        fake.calls().patches.is_empty(),
        "a revision change never replays a pending write"
    );
}

#[gpui::test]
fn validated_new_revision_reloads_the_active_overview_resource(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    let fake = Arc::new(FakeDelivery03Backend::default());
    fake.queue_inventory(Ok(vec![inventory_item()]));
    let mut revised = inventory_item();
    revised.version = Observed::Observed("5.4.0".into());
    fake.queue_inventory(Ok(vec![revised]));
    let services = AppServices::default().with_delivery03_backend(fake.clone());
    let capture = Rc::new(RefCell::new(None));
    let capture_view = capture.clone();
    let (_root, cx) = cx.add_window_view(move |window, cx| {
        let inspector = cx.new(|cx| BrokerInspector::new(window, services, 1, identity(), BrokerTab::Overview, cx));
        capture_view.replace(Some(inspector.clone()));
        Root::new(inspector, window, cx)
    });
    let inspector = capture.borrow_mut().take().expect("inspector entity");
    cx.run_until_parked();

    cx.update(|_, app| {
        inspector.update(app, |inspector, cx| {
            inspector.set_revision(2, cx);
            assert!(inspector.store.stale);
            assert!(matches!(inspector.store.overview.state, Loadable::Idle));
            // The owning inventory has validated the complete identity for revision 2.
            inspector.set_stale(false, cx);
        });
    });
    cx.run_until_parked();
    cx.read(|app| {
        let inspector = inspector.read(app);
        assert!(inspector.store.is_validated_for(2));
        assert!(matches!(
            &inspector.store.overview.state,
            Loadable::Ready(item) if item.version == Observed::Observed("5.4.0".into())
        ));
    });
    assert_eq!(fake.calls().inventory_revisions, [1, 2]);
}

#[gpui::test]
fn conflict_and_failure_keep_the_edit_draft_open_for_recovery(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    let fake = Arc::new(FakeDelivery03Backend::default());
    fake.queue_inventory(Ok(vec![inventory_item()]));
    fake.queue_config(Ok(config(7, "ASYNC_FLUSH")));
    fake.queue_patch(Ok(BrokerConfigMutationResult::GenerationConflict {
        expected_generation: 7,
        actual_generation: 8,
    }));
    fake.queue_patch(Err(UiError::new(
        "Broker config unavailable",
        UiErrorCode::Connection,
        true,
    )));
    let services = AppServices::default().with_delivery03_backend(fake.clone());
    let capture = Rc::new(RefCell::new(None));
    let capture_view = capture.clone();
    let (_root, cx) = cx.add_window_view(move |window, cx| {
        let inspector =
            cx.new(|cx| BrokerInspector::new(window, services, 3, identity(), BrokerTab::Configuration, cx));
        capture_view.replace(Some(inspector.clone()));
        Root::new(inspector, window, cx)
    });
    let inspector = capture.borrow_mut().take().expect("inspector entity");
    cx.run_until_parked();

    cx.update(|window, app| {
        inspector.update(app, |inspector, cx| {
            inspector
                .store
                .set_draft_value("flushDiskType", "SYNC_FLUSH".into())
                .expect("draft");
            inspector.submit_config(window, cx);
        });
    });
    cx.run_until_parked();
    cx.update(|window, app| {
        let inspector = inspector.read(app);
        assert!(matches!(
            inspector.store.submission,
            ConfigSubmissionState::GenerationConflict {
                expected_generation: 7,
                actual_generation: 8
            }
        ));
        assert_eq!(inspector.store.draft()["flushDiskType"], "SYNC_FLUSH");
        assert!(window.has_active_dialog(app));
        window.close_all_dialogs(app);
    });

    cx.update(|window, app| {
        inspector.update(app, |inspector, cx| inspector.submit_config(window, cx));
    });
    cx.run_until_parked();
    cx.update(|window, app| {
        let inspector = inspector.read(app);
        assert!(matches!(inspector.store.submission, ConfigSubmissionState::Failed(_)));
        assert_eq!(inspector.store.draft()["flushDiskType"], "SYNC_FLUSH");
        assert!(window.has_active_dialog(app));
    });
    let calls = fake.calls();
    assert_eq!(calls.patches.len(), 2);
    assert_eq!(calls.patches[0].1.entries()["flushDiskType"], "SYNC_FLUSH");
    assert!(!format!("{:?}", calls.patches).contains("must-not-cross-the-seam"));
}

#[gpui::test]
fn successful_edit_uses_authoritative_reload_truth_instead_of_merging_the_patch(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    let fake = Arc::new(FakeDelivery03Backend::default());
    fake.queue_inventory(Ok(vec![inventory_item()]));
    fake.queue_config(Ok(config(7, "ASYNC_FLUSH")));
    fake.queue_patch(Ok(BrokerConfigMutationResult::Applied {
        previous_generation: 7,
        snapshot: config(8, "SYNC_FLUSH_SERVER_NORMALIZED"),
        invalidations: Vec::new(),
    }));
    let services = AppServices::default().with_delivery03_backend(fake.clone());
    let capture = Rc::new(RefCell::new(None));
    let capture_view = capture.clone();
    let (_root, cx) = cx.add_window_view(move |window, cx| {
        let inspector =
            cx.new(|cx| BrokerInspector::new(window, services, 3, identity(), BrokerTab::Configuration, cx));
        capture_view.replace(Some(inspector.clone()));
        Root::new(inspector, window, cx)
    });
    let inspector = capture.borrow_mut().take().expect("inspector entity");
    cx.run_until_parked();

    cx.update(|window, app| {
        inspector.update(app, |inspector, cx| {
            inspector
                .store
                .set_draft_value("flushDiskType", "SYNC_FLUSH".into())
                .expect("draft");
            inspector.submit_config(window, cx);
        });
    });
    cx.run_until_parked();
    cx.read(|app| {
        let inspector = inspector.read(app);
        assert!(matches!(
            inspector.store.submission,
            ConfigSubmissionState::Succeeded { generation: 8 }
        ));
        assert_eq!(
            inspector
                .store
                .config
                .state
                .value()
                .expect("authoritative config")
                .entries()["flushDiskType"],
            "SYNC_FLUSH_SERVER_NORMALIZED"
        );
        assert_eq!(inspector.store.draft()["flushDiskType"], "SYNC_FLUSH_SERVER_NORMALIZED");
    });
    let calls = fake.calls();
    assert_eq!(calls.patches.len(), 1);
    assert_eq!(calls.patches[0].1.entries()["flushDiskType"], "SYNC_FLUSH");
}

#[gpui::test]
fn official_editor_reconciles_conflict_reload_keys_and_retries_with_the_new_generation(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    let fake = Arc::new(FakeDelivery03Backend::default());
    fake.queue_inventory(Ok(vec![inventory_item()]));
    fake.queue_config(Ok(config_entries(
        7,
        [
            ("flushDiskType", "ASYNC_FLUSH"),
            ("removedSetting", "old"),
            ("accessKey", "must-not-cross-the-seam"),
        ],
    )));
    fake.queue_patch(Ok(BrokerConfigMutationResult::GenerationConflict {
        expected_generation: 7,
        actual_generation: 8,
    }));
    fake.queue_config(Ok(config_entries(
        8,
        [
            ("flushDiskType", "ASYNC_FLUSH_SERVER"),
            ("newSetting", "server-default"),
            ("accessKey", "must-not-cross-the-seam"),
        ],
    )));
    fake.queue_patch(Ok(BrokerConfigMutationResult::Applied {
        previous_generation: 8,
        snapshot: config_entries(
            9,
            [
                ("flushDiskType", "SYNC_FLUSH_SERVER_NORMALIZED"),
                ("newSetting", "client-value-server-normalized"),
            ],
        ),
        invalidations: Vec::new(),
    }));
    let services = AppServices::default().with_delivery03_backend(fake.clone());
    let capture = Rc::new(RefCell::new(None));
    let capture_view = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let inspector =
            cx.new(|cx| BrokerInspector::new(window, services, 3, identity(), BrokerTab::Configuration, cx));
        capture_view.replace(Some(inspector.clone()));
        let harness = cx.new(|_| DialogHarness { inspector });
        Root::new(harness, window, cx)
    });
    let inspector = capture.borrow_mut().take().expect("inspector entity");
    cx.run_until_parked();

    cx.update(|window, app| {
        inspector.update(app, |inspector, cx| inspector.open_config_editor(window, cx));
        assert!(window.has_active_dialog(app));
    });
    cx.run_until_parked();
    cx.draw(point(px(0.), px(0.)), size(px(1_200.), px(900.)), |_, _| root.clone());
    cx.draw(point(px(0.), px(0.)), size(px(1_200.), px(900.)), |_, _| root.clone());
    let (editor_id, flush_input_id) = cx.update(|window, app| {
        let editor = inspector
            .read(app)
            .config_editor
            .clone()
            .expect("official config editor");
        let flush = editor.read(app).inputs["flushDiskType"].clone();
        flush.update(app, |input, cx| input.set_value("SYNC_FLUSH", window, cx));
        (editor.entity_id(), flush.entity_id())
    });

    click_debug(cx, "broker-config-editor-review");
    cx.draw(point(px(0.), px(0.)), size(px(1_200.), px(900.)), |_, _| root.clone());
    click_debug(cx, "confirm-dialog-ok");
    cx.run_until_parked();
    cx.update(|window, app| {
        assert!(
            window.has_active_dialog(app),
            "conflict recovery must remain in the official dialog stack"
        );
        assert!(matches!(
            inspector.read(app).store.submission,
            ConfigSubmissionState::GenerationConflict { .. }
        ));
    });

    cx.draw(point(px(0.), px(0.)), size(px(1_200.), px(900.)), |_, _| root.clone());
    click_debug(cx, "confirm-dialog-ok");
    cx.run_until_parked();
    cx.draw(point(px(0.), px(0.)), size(px(1_200.), px(900.)), |_, _| root.clone());
    cx.update(|window, app| {
        assert!(
            window.has_active_dialog(app),
            "the stable editor remains open after reload"
        );
        let editor = inspector
            .read(app)
            .config_editor
            .clone()
            .expect("reconciled official editor");
        assert_eq!(editor.entity_id(), editor_id);
        let editor = editor.read(app);
        assert_eq!(editor.inputs["flushDiskType"].entity_id(), flush_input_id);
        assert_eq!(editor.inputs["flushDiskType"].read(app).value(), "SYNC_FLUSH");
        assert!(!editor.inputs.contains_key("removedSetting"));
        assert!(editor.inputs.contains_key("newSetting"));
        assert_eq!(editor.inputs["newSetting"].read(app).value(), "server-default");
    });
    cx.update(|window, app| {
        let editor = inspector
            .read(app)
            .config_editor
            .clone()
            .expect("reconciled official editor");
        let added = editor.read(app).inputs["newSetting"].clone();
        added.update(app, |input, cx| input.set_value("client-value", window, cx));
    });

    click_debug(cx, "broker-config-editor-review");
    cx.draw(point(px(0.), px(0.)), size(px(1_200.), px(900.)), |_, _| root.clone());
    click_debug(cx, "confirm-dialog-ok");
    cx.run_until_parked();

    let calls = fake.calls();
    assert_eq!(calls.patches.len(), 2);
    assert_eq!(calls.patches[1].1.expected_generation, 8);
    assert_eq!(calls.patches[1].1.entries()["flushDiskType"], "SYNC_FLUSH");
    assert_eq!(calls.patches[1].1.entries()["newSetting"], "client-value");
    assert!(!calls.patches[1].1.entries().contains_key("removedSetting"));
    cx.update(|window, app| {
        assert!(!window.has_active_dialog(app));
        let inspector = inspector.read(app);
        assert!(matches!(
            inspector.store.submission,
            ConfigSubmissionState::Succeeded { generation: 9 }
        ));
        assert_eq!(inspector.store.draft()["newSetting"], "client-value-server-normalized");
    });
}
