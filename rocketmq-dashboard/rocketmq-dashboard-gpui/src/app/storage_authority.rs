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

//! Event-driven application of configuration observed by native storage/session refreshes.

use super::*;

impl RocketmqDashboard {
    pub(super) fn subscribe_configuration(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(mut changes) = self.services.configuration_changes() else {
            return;
        };
        let services = self.services.clone();
        self.configuration_watch_task = Some(cx.spawn_in(window, async move |this, cx| {
            loop {
                // Subscribe after startup and inspect the latest stamp immediately. A peer may
                // have changed it while bootstrap was installing the initial provider.
                let change = changes.borrow_and_update().clone();
                let refresh = this
                    .update_in(cx, |dashboard, _, _| {
                        matches!(dashboard.startup_state, StartupState::Ready(_))
                            && change.as_ref().is_some_and(|change| {
                                dashboard.configuration_revision != change.revision
                                    || dashboard.environment_id.as_ref() != Some(&change.environment_id)
                            })
                    })
                    .unwrap_or(false);
                if !refresh {
                    if changes.changed().await.is_err() {
                        break;
                    }
                    continue;
                }
                let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
                let work = services.refresh_authority_if_changed(sender);
                let delivery = async {
                    while let Some(event) = receiver.recv().await {
                        if this
                            .update_in(cx, |dashboard, window, cx| {
                                dashboard.handle_config_updated(event, window, cx)
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                };
                let (result, ()) = tokio::join!(work, delivery);
                if let Err(error) = result {
                    let _ = this.update_in(cx, |dashboard, _, cx| {
                        dashboard.last_service_error = Some(error);
                        cx.notify();
                    });
                }
                if changes.changed().await.is_err() {
                    break;
                }
            }
        }));
    }
}
