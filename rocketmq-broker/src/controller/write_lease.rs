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

use std::time::Duration;

use rocketmq_protocol::protocol::body::controller_write_lease::ControllerWriteLeaseGrant;
use rocketmq_store_api::MasterEpoch;
use rocketmq_store_api::WriteAuthority;
use rocketmq_store_api::WriteLeaseToken;

pub(crate) const MAX_APPEND_ACK_MILLIS: u64 = 1_000;

pub(crate) fn validate_grant(
    grant: ControllerWriteLeaseGrant,
    expected_authority: WriteAuthority,
    request_elapsed: Duration,
) -> Option<(WriteLeaseToken, Duration)> {
    if grant.lease_duration_millis <= grant.safety_margin_millis || grant.safety_margin_millis < MAX_APPEND_ACK_MILLIS {
        return None;
    }
    let master_epoch = MasterEpoch::try_from(grant.master_epoch).ok()?;
    let authority = WriteAuthority::try_new(grant.broker_id, master_epoch).ok()?;
    if authority != expected_authority {
        return None;
    }
    let token = WriteLeaseToken::try_new(authority, grant.generation).ok()?;
    let local_budget = Duration::from_millis(grant.lease_duration_millis - grant.safety_margin_millis);
    let valid_for = local_budget.checked_sub(request_elapsed)?;
    (!valid_for.is_zero()).then_some((token, valid_for))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn authority() -> WriteAuthority {
        WriteAuthority::try_new(0, MasterEpoch::try_from(3).unwrap()).unwrap()
    }

    fn grant() -> ControllerWriteLeaseGrant {
        ControllerWriteLeaseGrant {
            broker_id: 0,
            master_epoch: 3,
            generation: 7,
            lease_duration_millis: 10_000,
            safety_margin_millis: 2_000,
        }
    }

    #[test]
    fn request_send_time_bounds_the_local_deadline() {
        let (token, valid_for) = validate_grant(grant(), authority(), Duration::from_millis(1_500)).unwrap();
        assert_eq!(token.generation(), 7);
        assert_eq!(valid_for, Duration::from_millis(6_500));
        assert!(validate_grant(grant(), authority(), Duration::from_millis(8_000)).is_none());
    }

    #[test]
    fn wrong_authority_or_unsafe_margin_is_rejected() {
        let other = WriteAuthority::try_new(1, MasterEpoch::try_from(3).unwrap()).unwrap();
        assert!(validate_grant(grant(), other, Duration::ZERO).is_none());
        let mut unsafe_grant = grant();
        unsafe_grant.safety_margin_millis = 999;
        assert!(validate_grant(unsafe_grant, authority(), Duration::ZERO).is_none());
    }
}
