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

//! Native workspace recovery and the reusable next-launch engine preferences.

#[path = "storage_setup_render.rs"]
mod presentation;

use std::path::PathBuf;

use gpui::{Context, EventEmitter, FocusHandle, Task, Window};
use rocketmq_dashboard_storage::StorageEngine;

use crate::{
    components::dialog,
    services::{
        AppServices,
        storage_setup::{SelectionSource, StorageSetupSnapshot, WorkspaceSetupAction},
    },
    state::{UiError, UiErrorCode},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StorageSetupMode {
    Recovery,
    Preferences,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum StorageSetupIntent {
    Completed,
    Back,
}

pub(crate) struct StorageSetupView {
    services: AppServices,
    mode: StorageSetupMode,
    snapshot: Option<StorageSetupSnapshot>,
    draft_engine: StorageEngine,
    source_path: PathBuf,
    loading: bool,
    busy: bool,
    must_refresh: bool,
    error: Option<UiError>,
    notice: Option<String>,
    read_epoch: u64,
    status_task: Option<Task<()>>,
    mutation_task: Option<Task<()>>,
    chooser_task: Option<Task<()>>,
    focus_handle: FocusHandle,
}

impl EventEmitter<StorageSetupIntent> for StorageSetupView {}

impl StorageSetupView {
    pub(crate) fn new(services: AppServices, mode: StorageSetupMode, cx: &mut Context<Self>) -> Self {
        Self {
            services,
            mode,
            snapshot: None,
            draft_engine: StorageEngine::File,
            source_path: PathBuf::new(),
            loading: false,
            busy: false,
            must_refresh: false,
            error: None,
            notice: None,
            read_epoch: 0,
            status_task: None,
            mutation_task: None,
            chooser_task: None,
            focus_handle: cx.focus_handle().tab_stop(false),
        }
    }

    pub(crate) fn focus(&self, window: &mut Window) {
        self.focus_handle.focus(window);
    }

    pub(crate) fn sync_snapshot(&mut self, snapshot: StorageSetupSnapshot, cx: &mut Context<Self>) {
        self.status_task = None;
        self.read_epoch = self.read_epoch.saturating_add(1);
        if self.snapshot.is_none() {
            self.draft_engine = snapshot.saved_engine.or(snapshot.current_engine).unwrap_or_default();
            self.source_path = snapshot.legacy_config_path.clone();
        }
        self.snapshot = Some(snapshot);
        self.loading = false;
        self.must_refresh = false;
        cx.notify();
    }

    pub(crate) fn status_failed(&mut self, error: UiError, cx: &mut Context<Self>) {
        self.status_task = None;
        self.read_epoch = self.read_epoch.saturating_add(1);
        self.loading = false;
        if let Some(snapshot) = &mut self.snapshot {
            snapshot.status = Err(error);
        } else {
            self.error = Some(error);
        }
        cx.notify();
    }

    pub(crate) fn refresh(&mut self, cx: &mut Context<Self>) {
        if self.busy {
            return;
        }
        self.status_task = None;
        self.read_epoch = self.read_epoch.saturating_add(1);
        let epoch = self.read_epoch;
        self.loading = true;
        self.error = None;
        let services = self.services.clone();
        self.status_task = Some(cx.spawn(async move |this, cx| {
            let result = services.storage_setup_snapshot().await;
            let _ = this.update(cx, |view, cx| {
                if view.read_epoch != epoch {
                    return;
                }
                view.loading = false;
                match result {
                    Ok(snapshot) => view.sync_snapshot(snapshot, cx),
                    Err(error) => {
                        view.snapshot = None;
                        view.error = Some(error);
                        cx.notify();
                    }
                }
            });
        }));
        cx.notify();
    }

    fn can_save(&self) -> bool {
        !self.loading
            && !self.busy
            && self.draft_engine.is_compiled()
            && self.snapshot.as_ref().is_some_and(|snapshot| {
                snapshot.source != SelectionSource::Environment
                    && (snapshot.selection_error.is_some() || snapshot.saved_engine != Some(self.draft_engine))
            })
    }

    fn can_setup(&self) -> bool {
        self.mode == StorageSetupMode::Recovery
            && !self.loading
            && !self.busy
            && !self.must_refresh
            && self.snapshot.as_ref().is_some_and(|snapshot| {
                snapshot
                    .status
                    .as_ref()
                    .is_ok_and(|status| !status.initialized && status.engine != StorageEngine::File)
            })
    }

    fn initialized(&self) -> bool {
        self.snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.status.as_ref().is_ok_and(|status| status.initialized))
    }

    fn confirm_save(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.can_save() {
            return;
        }
        let engine = self.draft_engine;
        let owner = cx.entity().downgrade();
        dialog::open_confirm(
            "Save engine for next launch?",
            format!(
                "Use {} after restarting Dashboard. The current authority and its data stay active for this run. This replaces the local engine-choice file only.",
                presentation::engine_label(engine)
            ),
            "Save choice",
            move |_, _, cx| {
                let _ = owner.update(cx, |view, cx| view.save_engine(engine, cx));
                true
            },
            window,
            cx,
        );
    }

    fn save_engine(&mut self, engine: StorageEngine, cx: &mut Context<Self>) {
        if self.busy || self.loading {
            return;
        }
        self.busy = true;
        self.error = None;
        let services = self.services.clone();
        self.mutation_task = Some(cx.spawn(async move |this, cx| {
            let result = services.save_storage_engine(engine).await;
            let _ = this.update(cx, |view, cx| {
                view.busy = false;
                match result {
                    Ok(()) => {
                        view.notice = Some("Engine choice saved. Quit and reopen Dashboard to apply it; the active authority is unchanged.".into());
                        view.refresh(cx);
                    }
                    Err(error) => view.error = Some(error),
                }
                cx.notify();
            });
        }));
        cx.notify();
    }

    fn confirm_setup(&mut self, action: WorkspaceSetupAction, window: &mut Window, cx: &mut Context<Self>) {
        if !self.can_setup() {
            return;
        }
        let (title, message, button) = match &action {
            WorkspaceSetupAction::Initialize => ("Initialize workspace?", "Create empty native Dashboard records in the connected database. No existing workspace will be overwritten. PostgreSQL and MySQL databases must already exist; this action does not provision a database.".to_owned(), "Initialize workspace"),
            WorkspaceSetupAction::ImportLegacy { config_path } => ("Import legacy workspace?", format!("Import configuration, environments, Monitor, Audit, History and Sessions from {} and its sibling history.json / sessions.json into the connected empty workspace. Source files remain unchanged. Close any Dashboard instance using the source directory first.", config_path.display()), "Import workspace"),
        };
        let owner = cx.entity().downgrade();
        dialog::open_confirm(
            title,
            message,
            button,
            move |_, _, cx| {
                let _ = owner.update(cx, |view, cx| view.run_setup(action.clone(), cx));
                true
            },
            window,
            cx,
        );
    }

    fn run_setup(&mut self, action: WorkspaceSetupAction, cx: &mut Context<Self>) {
        if !self.can_setup() {
            return;
        }
        self.busy = true;
        self.error = None;
        self.status_task = None;
        self.read_epoch = self.read_epoch.saturating_add(1);
        let services = self.services.clone();
        self.mutation_task = Some(cx.spawn(async move |this, cx| {
            let result = services.complete_workspace_setup(action).await;
            let _ = this.update(cx, |view, cx| {
                view.busy = false;
                match result {
                    Ok(()) => cx.emit(StorageSetupIntent::Completed),
                    Err(error) => {
                        view.error = Some(error);
                        view.must_refresh = true;
                    }
                }
                cx.notify();
            });
        }));
        cx.notify();
    }

    fn choose_source(&mut self, cx: &mut Context<Self>) {
        if !self.can_setup() {
            return;
        }
        let chooser = cx.prompt_for_paths(gpui::PathPromptOptions {
            files: true,
            directories: false,
            multiple: false,
            prompt: Some("Choose legacy Dashboard configuration".into()),
        });
        self.chooser_task = Some(cx.spawn(async move |this, cx| {
            let selected = chooser.await;
            let _ = this.update(cx, |view, cx| {
                if !view.can_setup() {
                    return;
                }
                match selected {
                    Ok(Ok(Some(paths))) => {
                        if let Some(path) = paths.into_iter().next() {
                            view.source_path = path;
                        }
                    }
                    Ok(Ok(None)) => {}
                    _ => {
                        view.error = Some(UiError::new(
                            "The legacy file chooser could not be opened. The import source is unchanged.",
                            UiErrorCode::Configuration,
                            true,
                        ))
                    }
                }
                cx.notify();
            });
        }));
    }
}

#[cfg(test)]
#[path = "storage_setup_tests.rs"]
mod tests;
