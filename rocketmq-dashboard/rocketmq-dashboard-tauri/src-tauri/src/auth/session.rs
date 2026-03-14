// Copyright 2023 The RocketMQ Rust Authors
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

use crate::auth::types::SessionUser;
use crate::auth::types::UserRecord;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Mutex;
use uuid::Uuid;

#[derive(Debug, Default)]
pub(crate) struct SessionState {
    sessions: Mutex<HashMap<String, SessionUser>>,
}

impl SessionState {
    pub(crate) fn create_session(&self, user: &UserRecord) -> SessionUser {
        let session_user = SessionUser {
            session_id: Uuid::new_v4().to_string(),
            user_id: user.id,
            username: user.username.clone(),
            must_change_password: user.must_change_password,
            created_at: Utc::now().to_rfc3339(),
        };

        let mut sessions = self.sessions.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        sessions.insert(session_user.session_id.clone(), session_user.clone());
        session_user
    }

    pub(crate) fn get_session(&self, session_id: &str) -> Option<SessionUser> {
        let sessions = self.sessions.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        sessions.get(session_id).cloned()
    }

    pub(crate) fn remove_session(&self, session_id: &str) -> bool {
        let mut sessions = self.sessions.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        sessions.remove(session_id).is_some()
    }

    pub(crate) fn upsert_session(&self, session: SessionUser) {
        let mut sessions = self.sessions.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        sessions.insert(session.session_id.clone(), session);
    }

    pub(crate) fn mark_password_changed(&self, session_id: &str) -> Option<SessionUser> {
        let mut sessions = self.sessions.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let session = sessions.get_mut(session_id)?;
        session.must_change_password = false;
        Some(session.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::SessionState;
    use crate::auth::types::UserRecord;

    fn test_user_record() -> UserRecord {
        UserRecord {
            id: 7,
            username: "admin".to_string(),
            password_hash: "hashed".to_string(),
            is_active: true,
            must_change_password: true,
        }
    }

    #[test]
    fn session_lifecycle_round_trip() {
        let state = SessionState::default();
        let user = test_user_record();
        let session = state.create_session(&user);

        let restored = state
            .get_session(&session.session_id)
            .expect("session should be restorable");
        assert_eq!(restored.user_id, user.id);
        assert_eq!(restored.username, user.username);
        assert!(state.remove_session(&session.session_id));
        assert!(state.get_session(&session.session_id).is_none());
    }

    #[test]
    fn mark_password_changed_updates_session_state() {
        let state = SessionState::default();
        let user = test_user_record();
        let session = state.create_session(&user);

        let updated = state
            .mark_password_changed(&session.session_id)
            .expect("session should exist");

        assert!(!updated.must_change_password);
    }
}
