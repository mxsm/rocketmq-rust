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

//! Shared authentication strategy helpers.

use std::collections::HashSet;

/// Narrow common behavior shared by the concrete authentication strategies.
pub trait AbstractAuthenticationStrategy: Send + Sync {
    /// Get the authentication whitelist set.
    fn authentication_white_set(&self) -> &HashSet<String>;

    /// Check if RPC code is whitelisted.
    fn is_whitelisted(&self, rpc_code: &str) -> bool {
        self.authentication_white_set().contains(rpc_code)
    }
}
