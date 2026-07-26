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

use rocketmq_auth::AuthConfig;
use rocketmq_auth::AuthenticationStrategy;
use rocketmq_auth::AuthorizationStrategy;
use rocketmq_auth::Resource;
use rocketmq_auth::SecurityResource;

#[test]
fn auth_consumers_use_only_intentional_root_exports() {
    let source = include_str!("../src/lib.rs");
    for module in [
        "acl",
        "authentication",
        "authorization",
        "bootstrap",
        "config",
        "credential_rotation",
        "migration",
        "permission",
        "runtime",
        "secret_provider",
        "security_api",
    ] {
        assert!(
            !source.contains(&format!("pub mod {module}")),
            "`rocketmq-auth` implementation module `{module}` must remain private"
        );
    }

    fn accepts_authentication_strategy<T: AuthenticationStrategy>() {}
    fn accepts_authorization_strategy<T: AuthorizationStrategy>() {}
    fn accepts_domain_resource(_: Option<Resource>) {}
    fn accepts_security_resource(_: Option<SecurityResource>) {}

    let _ = AuthConfig::default();
    let _ = accepts_authentication_strategy::<rocketmq_auth::AllowAllAuthenticationStrategy>;
    let _ = accepts_authorization_strategy::<rocketmq_auth::StatelessAuthorizationStrategy>;
    accepts_domain_resource(None);
    accepts_security_resource(None);
}
