// Copyright 2025 The RocketMQ Rust Authors
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

//! Safe connection labels shown by the shell Topbar.

/// The only connection state rendered before Delivery 02 has a real runtime.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ConnectionSummary {
    configured: bool,
}

impl ConnectionSummary {
    /// Returns the explicit label required before a configuration exists.
    pub const fn label(self) -> &'static str {
        if self.configured {
            "Configuration available"
        } else {
            "Not configured"
        }
    }
}
