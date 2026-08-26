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

use std::time::Duration;

use rocketmq_security_api::Principal;
use rocketmq_transport::api::v1::RequestDeadline as V1RequestDeadline;
use rocketmq_transport::api::v1::RequestId as V1RequestId;
use rocketmq_transport::api::v2::AuthenticationState;
use rocketmq_transport::api::v2::EmbeddedCaller;
use rocketmq_transport::api::v2::OriginalRequestIdentity;
use rocketmq_transport::api::v2::RequestDeadline as V2RequestDeadline;
use rocketmq_transport::api::v2::RequestId as V2RequestId;
use rocketmq_transport::api::v2::RequestOrigin;

fn assert_same_deadline_type(_: &V1RequestDeadline, _: &V2RequestDeadline) {}

fn assert_same_request_id_type(value: Option<V1RequestId>) -> Option<V2RequestId> {
    value
}

fn assert_original_identity_contract(identity: Option<OriginalRequestIdentity>) {
    if let Some(identity) = identity {
        let _: V2RequestId = identity.request_id();
        let _: i32 = identity.original_code();
        let _: i32 = identity.original_opaque();
        let _: bool = identity.is_one_way();
    }
}

#[test]
fn v2_exposes_the_v1_request_deadline_type() {
    let deadline = V2RequestDeadline::after(Duration::from_secs(1));

    assert_same_deadline_type(&deadline, &deadline);
}

#[test]
fn v2_reuses_v1_request_id_and_exposes_read_only_original_identity() {
    let v1_value: Option<V1RequestId> = None;
    let v2_value: Option<V2RequestId> = assert_same_request_id_type(v1_value);
    assert!(v2_value.is_none());
    assert_original_identity_contract(None);
}

#[test]
fn v2_exposes_read_only_origin_and_authenticated_principal() {
    fn authenticated_principal(state: &AuthenticationState) -> Option<&Principal> {
        state.principal()
    }

    fn has_authenticated_principal(state: &AuthenticationState) -> bool {
        match state {
            AuthenticationState::Anonymous | AuthenticationState::SecurityDisabled => false,
            AuthenticationState::Authenticated(principal, ..) => principal.id() == "v2-user",
            _ => state.principal().is_some(),
        }
    }

    fn inspect_origin(origin: &RequestOrigin) -> Option<std::net::SocketAddr> {
        match origin {
            RequestOrigin::Network { peer, .. } => Some(peer.address()),
            RequestOrigin::Embedded { caller } => {
                let _known_caller = matches!(caller, EmbeddedCaller::BrokerProxy);
                None
            }
            _ => None,
        }
    }

    let _: fn(&AuthenticationState) -> Option<&Principal> = authenticated_principal;
    let _: fn(&AuthenticationState) -> bool = has_authenticated_principal;
    let _: fn(&RequestOrigin) -> Option<std::net::SocketAddr> = inspect_origin;
}
