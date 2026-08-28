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

use parking_lot::Mutex;

use crate::long_polling::pull_request::PullRequest;

pub struct ManyPullRequest {
    pull_request_list: Arc<Mutex<Vec<PullRequest>>>,
}

impl ManyPullRequest {
    pub fn new() -> Self {
        ManyPullRequest {
            pull_request_list: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn add_pull_request(&self, pull_request: PullRequest) {
        let mut list = self.pull_request_list.lock();
        list.push(pull_request);
    }

    pub fn add_pull_requests(&self, many: Vec<PullRequest>) {
        let mut list = self.pull_request_list.lock();
        list.extend(many);
    }

    pub fn clone_list_and_clear(&self) -> Option<Vec<PullRequest>> {
        let mut list = self.pull_request_list.lock();
        if !list.is_empty() {
            let result = list.clone();
            list.clear();
            Some(result)
        } else {
            None
        }
    }

    /// Removes only requests for which the caller acquired an affine claim.
    /// The claim is created while the request is still protected by the table
    /// node, closing the route-permit versus removal window.
    pub(crate) fn drain_with_claim<T>(
        &self,
        mut acquire: impl FnMut(&PullRequest) -> Option<T>,
    ) -> Vec<(PullRequest, T)> {
        let mut list = self.pull_request_list.lock();
        let requests = std::mem::take(&mut *list);
        let mut claimed = Vec::with_capacity(requests.len());
        for request in requests {
            if let Some(claim) = acquire(&request) {
                claimed.push((request, claim));
            } else {
                list.push(request);
            }
        }
        claimed
    }

    pub fn min_deadline_millis(&self) -> Option<u64> {
        let list = self.pull_request_list.lock();
        list.iter().map(PullRequest::deadline_millis).min()
    }

    pub(crate) fn remove_legacy_identity(&self, identity: u64) -> Option<PullRequest> {
        let mut list = self.pull_request_list.lock();
        let index = list
            .iter()
            .position(|request| request.legacy_handoff_identity() == identity)?;
        Some(list.remove(index))
    }

    pub fn is_empty(&self) -> bool {
        let list = self.pull_request_list.lock();
        list.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.pull_request_list.lock().len()
    }

    #[cfg(test)]
    pub(crate) fn take_first_legacy_wait(&self) -> Option<crate::deferred_generation_handoff::LegacyWaitLease> {
        self.pull_request_list.lock().first()?.take_legacy_wait()
    }
}
