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

    /*    pub fn get_pull_request_list(&self) -> Vec<PullRequest> {
        let list = self.pull_request_list.lock();
        list.clone()
    }*/

    pub fn is_empty(&self) -> bool {
        let list = self.pull_request_list.lock();
        list.is_empty()
    }
}
