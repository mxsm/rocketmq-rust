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

use rocketmq_auth::security_api::Principal as LegacyPrincipal;
use rocketmq_auth::security_api::Resource as LegacyResource;
use rocketmq_security_api::Principal;
use rocketmq_security_api::Resource;

#[test]
fn auth_compatibility_path_exports_canonical_security_types() {
    fn principal_identity(value: Principal) -> LegacyPrincipal {
        value
    }
    fn resource_identity(value: Resource) -> LegacyResource {
        value
    }

    assert_eq!(principal_identity(Principal::new("alice")).id(), "alice");
    assert_eq!(resource_identity(Resource::topic("TopicA")).name(), "TopicA");
}
