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

use std::sync::Weak;

use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::LanguageCode;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_transport::api::v2::ServerPushSender;
use rocketmq_transport::api::v2::ServerRequestSender;
use rocketmq_transport::api::v2::SessionCloseHandle;
use rocketmq_transport::api::v2::SessionCloseReason;
use rocketmq_transport::api::v2::SessionId;
use rocketmq_transport::api::v2::V2SessionRegistry;

/// Exact-generation transport capabilities retained for one registered V2 client.
///
/// The bundle deliberately exposes no raw connection or arbitrary command writer. Its
/// `SessionId` is checked when it is assembled so manager cleanup can remove exactly the
/// disconnected generation without affecting a reconnect using the same client id.
#[derive(Clone)]
pub(crate) struct ClientSessionTransport {
    session_id: SessionId,
    push: ServerPushSender,
    requests: ServerRequestSender,
    close: SessionCloseHandle,
}

impl ClientSessionTransport {
    pub(crate) fn resolve(registry: &V2SessionRegistry, session_id: SessionId) -> Option<Self> {
        let (push, close) = registry.capabilities(session_id)?;
        let requests = registry.server_request_sender(session_id)?;
        debug_assert_eq!(push.session_id(), session_id);
        debug_assert_eq!(requests.session_id(), session_id);
        debug_assert_eq!(close.session_id(), session_id);
        Some(Self {
            session_id,
            push,
            requests,
            close,
        })
    }

    pub(crate) const fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub(crate) fn push_sender(&self) -> ServerPushSender {
        self.push.clone()
    }

    pub(crate) fn request_sender(&self) -> ServerRequestSender {
        self.requests.clone()
    }

    pub(crate) fn close_handle(&self) -> SessionCloseHandle {
        self.close.clone()
    }

    pub(crate) fn retirement(&self, registry: Weak<V2SessionRegistry>) -> ClientSessionRetirement {
        ClientSessionRetirement {
            close: self.close_handle(),
            registry,
        }
    }
}

#[derive(Clone)]
pub(crate) struct ClientSessionRetirement {
    close: SessionCloseHandle,
    registry: Weak<V2SessionRegistry>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClientSessionRetirementOutcome {
    Graceful,
    Forced,
    AlreadyGone,
}

impl ClientSessionRetirement {
    pub(crate) fn session_id(&self) -> SessionId {
        self.close.session_id()
    }

    pub(crate) async fn retire(&self, reason: SessionCloseReason) -> ClientSessionRetirementOutcome {
        if self.close.close(reason).await.is_ok() {
            return ClientSessionRetirementOutcome::Graceful;
        }
        self.registry
            .upgrade()
            .map_or(ClientSessionRetirementOutcome::AlreadyGone, |registry| {
                if registry.close_now(self.session_id()) {
                    ClientSessionRetirementOutcome::Forced
                } else {
                    ClientSessionRetirementOutcome::AlreadyGone
                }
            })
    }
}

/// Client identity retained for a V2 transport session.
///
/// This value contains no transport writer, request, session view, or connection
/// context. The stable session identifier is the only link back to transport
/// lifecycle state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClientSessionInfo {
    session_id: SessionId,
    client_id: CheetahString,
    remote_address: Option<CheetahString>,
    language: LanguageCode,
    version: i32,
    last_update_timestamp: u64,
}

impl ClientSessionInfo {
    pub(crate) fn new(
        session_id: SessionId,
        client_id: CheetahString,
        remote_address: Option<CheetahString>,
        language: LanguageCode,
        version: i32,
    ) -> Self {
        Self {
            session_id,
            client_id,
            remote_address,
            language,
            version,
            last_update_timestamp: current_millis(),
        }
    }

    pub(crate) const fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub(crate) const fn client_id(&self) -> &CheetahString {
        &self.client_id
    }

    pub(crate) fn remote_address(&self) -> Option<&str> {
        self.remote_address.as_deref()
    }

    pub(crate) const fn language(&self) -> LanguageCode {
        self.language
    }

    pub(crate) const fn version(&self) -> i32 {
        self.version
    }

    pub(crate) const fn last_update_timestamp(&self) -> u64 {
        self.last_update_timestamp
    }

    pub(crate) fn refresh_from(&mut self, current: &Self) {
        debug_assert_eq!(
            self.client_id, current.client_id,
            "a live SessionId cannot change client identity"
        );
        self.remote_address.clone_from(&current.remote_address);
        self.language = current.language;
        self.version = current.version;
        self.last_update_timestamp = current_millis();
    }

    #[cfg(test)]
    pub(crate) fn set_last_update_timestamp_for_test(&mut self, timestamp: u64) {
        self.last_update_timestamp = timestamp;
    }
}
