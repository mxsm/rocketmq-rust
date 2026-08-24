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
    services::{AppServices, delivery03::test_support::FakeDelivery03Backend},
    state::{Loadable, UiError, UiErrorCode},
};

#[gpui::test]
fn initial_inventory_failure_remains_error_instead_of_becoming_empty(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    let fake = Arc::new(FakeDelivery03Backend::default());
    fake.queue_inventory(Err(UiError::new(
        "Broker inventory unavailable",
        UiErrorCode::Connection,
        true,
    )));
    let services = AppServices::default().with_delivery03_backend(fake.clone());
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
