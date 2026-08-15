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

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use parking_lot::Mutex;
use rocketmq_store_api::WriteLeaseToken;

#[derive(Clone, Copy, Debug)]
struct ActiveWriteLease {
    token: WriteLeaseToken,
    deadline: Instant,
}

/// Process-local monotonic projection of Controller write authority.
#[derive(Clone, Debug)]
pub(crate) struct ControllerWriteLeaseState {
    required: bool,
    active: Arc<Mutex<Option<ActiveWriteLease>>>,
}

impl ControllerWriteLeaseState {
    pub(crate) fn new(required: bool) -> Self {
        Self {
            required,
            active: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn install(&self, token: WriteLeaseToken, valid_for: Duration) -> bool {
        let Some(deadline) = Instant::now().checked_add(valid_for) else {
            return false;
        };
        if valid_for.is_zero() {
            return false;
        }

        let mut active = self.active.lock();
        if active
            .as_ref()
            .is_some_and(|current| current.token.generation() >= token.generation())
        {
            return false;
        }
        *active = Some(ActiveWriteLease { token, deadline });
        true
    }

    pub(crate) fn fence(&self) {
        if let Some(active) = self.active.lock().as_mut() {
            active.deadline = Instant::now();
        }
    }

    pub(crate) fn capture(&self) -> Result<Option<WriteLeaseToken>, ()> {
        if !self.required {
            return Ok(None);
        }
        let active = self.active.lock();
        active
            .as_ref()
            .filter(|lease| Instant::now() < lease.deadline)
            .map(|lease| Some(lease.token))
            .ok_or(())
    }

    pub(crate) fn validate(&self, expected: Option<WriteLeaseToken>) -> bool {
        if !self.required {
            return expected.is_none();
        }
        let Some(expected) = expected else {
            return false;
        };
        self.active
            .lock()
            .as_ref()
            .is_some_and(|lease| lease.token == expected && Instant::now() < lease.deadline)
    }

    pub(crate) fn is_write_permitted(&self) -> bool {
        self.capture().is_ok()
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use rocketmq_store_api::MasterEpoch;
    use rocketmq_store_api::WriteAuthority;

    use super::*;

    fn token(generation: u64) -> WriteLeaseToken {
        let authority = WriteAuthority::try_new(0, MasterEpoch::try_from(3).unwrap()).unwrap();
        WriteLeaseToken::try_new(authority, generation).unwrap()
    }

    #[test]
    fn expired_and_stale_leases_fail_closed() {
        let state = ControllerWriteLeaseState::new(true);
        assert!(state.capture().is_err());
        assert!(state.install(token(2), Duration::from_millis(2)));
        let captured = state.capture().unwrap();
        thread::sleep(Duration::from_millis(4));
        assert!(!state.validate(captured));
        assert!(!state.install(token(1), Duration::from_secs(1)));
        assert!(state.capture().is_err());
    }

    #[test]
    fn explicit_fence_invalidates_current_generation() {
        let state = ControllerWriteLeaseState::new(true);
        assert!(state.install(token(4), Duration::from_secs(1)));
        let captured = state.capture().unwrap();
        state.fence();
        assert!(!state.validate(captured));
    }
}
