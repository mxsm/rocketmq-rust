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

//! Owned validation for actions that expose already-cached protected data.

use gpui::{Context, Task, Window};

use crate::services::{AppServices, SessionState};

/// The caller retains this task and drops it when its data context is cleared. Validate before
/// exposing cached data, then recheck the captured local session and connection after the await.
pub(crate) fn run<T: 'static>(
    services: AppServices,
    session: Option<SessionState>,
    revision: u64,
    window: &mut Window,
    cx: &mut Context<T>,
    on_authorized: impl FnOnce(&mut T, &mut Window, &mut Context<T>) + 'static,
) -> Task<()> {
    cx.spawn_in(window, async move |this, cx| {
        if services.current_session() != session
            || (session.is_some() && services.connection_state().config.revision != revision)
        {
            return;
        }
        let result = services.protected_access_for(session.clone()).await;
        let _ = this.update_in(cx, |view, window, cx| {
            if services.current_session() != session
                || (session.is_some() && services.connection_state().config.revision != revision)
            {
                return;
            }
            match result {
                Ok(access) if services.protected_access_is_current(&access) => on_authorized(view, window, cx),
                Ok(_) => {}
                Err(error) => super::toast::ToastHost::error(error.summary().to_owned(), window, cx),
            }
        });
    })
}

pub(crate) fn copy<T: 'static>(
    services: AppServices,
    session: Option<SessionState>,
    revision: u64,
    value: String,
    window: &mut Window,
    cx: &mut Context<T>,
) -> Task<()> {
    run(services, session, revision, window, cx, move |_, _, cx| {
        cx.write_to_clipboard(gpui::ClipboardItem::new_string(value))
    })
}
