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

//! Route-operation result and canonical constructors.

use rocketmq_error::SharedError;

/// Result type for NameServer route operations.
pub type RouteResult<T> = std::result::Result<T, SharedError>;

pub(crate) use crate::namesrv_error::cluster_not_found;

#[cfg(test)]
mod tests {
    use rocketmq_error::ROUTE_CLUSTER_NOT_FOUND;

    use super::*;

    #[test]
    fn cluster_error_uses_catalog_identity_and_stable_remoting_code() {
        let cluster = cluster_not_found("TestCluster");
        assert_eq!(cluster.code(), ROUTE_CLUSTER_NOT_FOUND.code());
        assert_eq!(cluster.projection().remoting().code.as_i32(), 211);
    }
}
