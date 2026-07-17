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

use std::fmt;

/// A namespace-qualified RocketMQ resource name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResourceIdentity {
    namespace: String,
    name: String,
}

impl ResourceIdentity {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
        }
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl fmt::Display for ResourceIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.namespace.is_empty() {
            write!(f, "{}", self.name)
        } else {
            write!(f, "{}%{}", self.namespace, self.name)
        }
    }
}
