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

//! Error conversion shared by the read and mutation client adapters.

pub(crate) trait IntoCanonicalError {
    fn into_canonical_error(self) -> rocketmq_error::Error;
}

impl IntoCanonicalError for rocketmq_error::Error {
    fn into_canonical_error(self) -> rocketmq_error::Error {
        self
    }
}

impl IntoCanonicalError for rocketmq_client_rust::ClientError {
    fn into_canonical_error(self) -> rocketmq_error::Error {
        self.into_error()
    }
}
