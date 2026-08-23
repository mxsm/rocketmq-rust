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

//! Safe real connection labels shown by the shell Topbar.

use gpui::{Pixels, px};
use std::fmt;

use rocketmq_dashboard_common::{AdminSessionStatus, ConnectionScope, EndpointAvailability};

use crate::services::{GlobalConnectionState, SessionState};

/// The fixed height shared by every Topbar layout branch.
pub const TOPBAR_HEIGHT: Pixels = px(56.);

/// Complete non-sensitive Topbar projection.
#[derive(Clone, PartialEq, Eq)]
pub struct ConnectionSummary {
    /// Persisted configuration generation rendered independently of endpoint values.
    pub revision: u64,
    /// Selected NameServer or explicit absence.
    pub nameserver: String,
    /// Active NameServer/Proxy query scope.
    pub scope: String,
    /// TLS state.
    pub tls: &'static str,
    /// Local dashboard session state.
    pub session: String,
    /// Real Admin provider lifecycle state for the same revision.
    pub admin_session: AdminSessionStatus,
    /// Current availability result.
    pub health: &'static str,
}

impl fmt::Debug for ConnectionSummary {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectionSummary")
            .field("revision", &self.revision)
            .field("nameserver_configured", &(self.nameserver != "Not configured"))
            .field("scope_configured", &!self.scope.contains("not configured"))
            .field("tls", &self.tls)
            .field("signed_in", &(self.session == "Signed in"))
            .field("admin_session", &self.admin_session)
            .field("health", &self.health)
            .finish()
    }
}

impl ConnectionSummary {
    /// Projects only safe global/session values.
    pub fn from_state(state: &GlobalConnectionState, session: &SessionState) -> Self {
        let nameserver = state
            .config
            .current_nameserver
            .clone()
            .unwrap_or_else(|| "Not configured".into());
        let scope = match state.config.scope {
            ConnectionScope::NameServer => "Scope: NameServer".into(),
            ConnectionScope::Proxy => state.config.current_proxy.as_ref().map_or_else(
                || "Scope: Proxy not configured".into(),
                |proxy| format!("Proxy: {proxy}"),
            ),
        };
        let session = if !state.config.auth.enabled {
            "Auth off".into()
        } else if session.is_authenticated() {
            "Signed in".into()
        } else {
            "Signed out".into()
        };
        let health = match state.health.as_ref().map(|health| health.availability) {
            None if state.session.status == AdminSessionStatus::Failed => "Unavailable",
            None | Some(EndpointAvailability::Unknown) => "Health unknown",
            Some(EndpointAvailability::Available) => "Available",
            Some(EndpointAvailability::Unavailable) => "Unavailable",
        };
        Self {
            revision: state.config.revision,
            nameserver,
            scope,
            tls: if state.config.transport.use_tls {
                "TLS on"
            } else {
                "TLS off"
            },
            session,
            admin_session: state.session.status.clone(),
            health,
        }
    }

    /// Concise lifecycle label used by the Topbar without formatting the full state.
    pub const fn admin_session_label(&self) -> &'static str {
        match self.admin_session {
            AdminSessionStatus::NotConfigured => "Admin: Not configured",
            AdminSessionStatus::Connecting => "Admin: Connecting",
            AdminSessionStatus::Connected => "Admin: Ready",
            AdminSessionStatus::Failed => "Admin: Failed",
            AdminSessionStatus::Closed => "Admin: Closed",
        }
    }

    /// Concise status projection for narrow windows; all three values remain real state.
    pub fn compact_label(&self) -> String {
        format!(
            "Rev {} · {} · {}",
            self.revision,
            self.admin_session_label(),
            self.health
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unconfigured_state_is_never_rendered_as_an_empty_badge() {
        let summary = ConnectionSummary::from_state(&GlobalConnectionState::default(), &SessionState::signed_out());

        assert_eq!(summary.nameserver, "Not configured");
        assert_eq!(summary.scope, "Scope: NameServer");
        assert_eq!(summary.session, "Auth off");
        assert_eq!(summary.admin_session, AdminSessionStatus::NotConfigured);
        assert_eq!(summary.health, "Health unknown");
    }

    #[test]
    fn projection_exposes_revision_and_real_session_failure_without_endpoint_debug() {
        let mut state = GlobalConnectionState::default();
        state.config.revision = 41;
        state.config.current_nameserver = Some("sensitive-name-server:9876".into());
        state.session.revision = 41;
        state.session.status = AdminSessionStatus::Connecting;
        let connecting = ConnectionSummary::from_state(&state, &SessionState::for_username("private-user".into()));

        assert_eq!(connecting.revision, 41);
        assert_eq!(connecting.admin_session_label(), "Admin: Connecting");
        assert_eq!(connecting.health, "Health unknown");

        state.session.status = AdminSessionStatus::Failed;
        let failed = ConnectionSummary::from_state(&state, &SessionState::for_username("private-user".into()));
        let debug = format!("{failed:?}");
        assert_eq!(failed.admin_session_label(), "Admin: Failed");
        assert_eq!(failed.health, "Unavailable");
        for value in ["sensitive-name-server:9876", "private-user"] {
            assert!(!debug.contains(value));
        }
    }

    #[test]
    fn compact_projection_retains_revision_admin_and_health_semantics() {
        let mut state = GlobalConnectionState::default();
        state.config.revision = 7;
        state.session.status = AdminSessionStatus::Connecting;
        let summary = ConnectionSummary::from_state(&state, &SessionState::signed_out());

        let compact = summary.compact_label();
        assert!(compact.contains("Rev 7"));
        assert!(compact.contains("Admin: Connecting"));
        assert!(compact.contains("Health unknown"));
    }
}
