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

//! Bounded immutable session pages within one creation window; navigation revalidates access.

use rocketmq_dashboard_storage::{SessionCursor, SessionQuery};

use crate::{
    services::sessions::SessionPageView,
    state::{UiError, UiErrorCode},
};

const PAGE_SIZE: usize = 25;
const MAX_CURSOR_HISTORY: usize = 400;

pub(super) struct SessionPageRequest {
    pub query: SessionQuery,
    pub cached: Option<SessionPageView>,
    index: usize,
}

pub(super) struct SessionFilter {
    pub username: Option<String>,
}

#[derive(Default)]
pub(super) struct SessionsStore {
    pub applied: Option<SessionFilter>,
    pub page: Option<SessionPageView>,
    pub page_index: usize,
    cursors: Vec<Option<SessionCursor>>,
    pages: Vec<SessionPageView>,
    pub loading: bool,
    pub error: Option<UiError>,
}

impl SessionsStore {
    pub fn apply(&mut self, username: Option<String>) -> SessionPageRequest {
        self.applied = Some(SessionFilter {
            username: username.clone(),
        });
        self.page = None;
        self.page_index = 0;
        self.cursors = vec![None];
        self.pages.clear();
        self.error = None;
        SessionPageRequest {
            query: SessionQuery {
                username,
                cursor: None,
                limit: PAGE_SIZE,
            },
            cached: None,
            index: 0,
        }
    }

    pub fn next(&self) -> Option<SessionPageRequest> {
        if self.page_index + 1 >= MAX_CURSOR_HISTORY {
            return None;
        }
        Some(SessionPageRequest {
            query: SessionQuery {
                username: self.applied.as_ref()?.username.clone(),
                cursor: Some(self.page.as_ref()?.next_cursor.clone()?),
                limit: PAGE_SIZE,
            },
            cached: self.pages.get(self.page_index + 1).cloned(),
            index: self.page_index + 1,
        })
    }

    pub fn previous(&self) -> Option<SessionPageRequest> {
        let index = self.page_index.checked_sub(1)?;
        Some(SessionPageRequest {
            query: SessionQuery {
                username: self.applied.as_ref()?.username.clone(),
                cursor: self.cursors.get(index)?.clone(),
                limit: PAGE_SIZE,
            },
            cached: self.pages.get(index).cloned(),
            index,
        })
    }

    pub fn finish(&mut self, request: SessionPageRequest, result: Result<SessionPageView, UiError>) {
        self.loading = false;
        match result {
            Ok(page) => {
                if page.next_cursor.is_some() && page.next_cursor == request.query.cursor {
                    self.error = Some(UiError::new(
                        "The session cursor did not advance. Restart the query.",
                        UiErrorCode::Validation,
                        false,
                    ));
                    return;
                }
                if request.cached.is_none() {
                    self.cursors.truncate(request.index + 1);
                    if self.cursors.len() == request.index {
                        self.cursors.push(request.query.cursor);
                    }
                    self.pages.truncate(request.index);
                    self.pages.push(page.clone());
                }
                self.page_index = request.index;
                self.page = Some(page);
                self.error = None;
            }
            Err(error) => self.error = Some(error),
        }
    }

    pub fn clear(&mut self) {
        *self = Self::default();
    }
}
