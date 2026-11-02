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

//! Peer configuration uses the existing ordered provider and view invalidation pipeline.

use super::*;

impl AppServices {
    pub(crate) fn configuration_changes(
        &self,
    ) -> Option<tokio::sync::watch::Receiver<Option<crate::infrastructure::config_store::NativeConfigurationChange>>>
    {
        self.backend.configuration_changes()
    }

    pub(crate) async fn refresh_authority_if_changed(
        &self,
        progress: ConfigProgressSender,
    ) -> Result<Option<ConfigUpdate>, UiError> {
        let backend = Arc::clone(&self.backend);
        let work = async move { backend.refresh_authority_if_changed(progress).await };
        match &self.runtime_bridge {
            Some(runtime) => runtime.run("gpui-authority-refresh", work).await,
            None => work.await,
        }
    }
}

impl DesktopBackend {
    pub(super) async fn reload_peer_configuration(
        &self,
        progress: ConfigProgressSender,
    ) -> Result<Option<ConfigUpdate>, UiError> {
        let _update = self.update_gate.lock().await;
        let config = match self.store.load().await {
            Ok(config) => config,
            Err(error) => {
                // The existing bounded native-session refresh supplies the next read. Clear only
                // the observation marker so a recovered, unchanged revision can notify the UI.
                self.store.retry_configuration_observation();
                return Err(config_ui_error(error));
            }
        };
        let old = self.state.read().config.clone();
        let catalog = self.store.environments();
        let same_environment = self
            .state
            .read()
            .environments
            .as_ref()
            .map(|catalog| &catalog.active_id)
            == catalog.as_ref().map(|catalog| &catalog.active_id);
        if config == old && same_environment {
            self.state.write().environments = catalog;
            return Ok(None);
        }
        let transition = ConfigMutation::Reload.route_transition(old.auth.enabled, config.auth.enabled);
        let warning = self
            .install_persisted(config.clone(), Some(&progress), transition)
            .await;
        self.reconcile_foundations(config.foundations).await?;
        send_progress(
            Some(&progress),
            config.revision,
            ConfigUpdatePhase::Completed,
            transition,
        );
        Ok(Some(ConfigUpdate {
            config,
            connection_warning: warning,
        }))
    }
}
