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

use cheetah_string::CheetahString;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NameserverAccessConfig {
    namesrv_addr: CheetahString,
    namesrv_domain: CheetahString,
    namesrv_domain_subgroup: CheetahString,
}

impl NameserverAccessConfig {
    pub fn new(
        namesrv_addr: impl Into<CheetahString>,
        namesrv_domain: impl Into<CheetahString>,
        namesrv_domain_subgroup: impl Into<CheetahString>,
    ) -> Self {
        Self {
            namesrv_addr: namesrv_addr.into(),
            namesrv_domain: namesrv_domain.into(),
            namesrv_domain_subgroup: namesrv_domain_subgroup.into(),
        }
    }

    #[inline]
    pub fn namesrv_addr(&self) -> &CheetahString {
        &self.namesrv_addr
    }

    #[inline]
    pub fn namesrv_domain(&self) -> &CheetahString {
        &self.namesrv_domain
    }

    #[inline]
    pub fn namesrv_domain_subgroup(&self) -> &CheetahString {
        &self.namesrv_domain_subgroup
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nameserver_access_config_matches_java_fields() {
        let config = NameserverAccessConfig::new("127.0.0.1:9876", "domain", "subgroup");

        assert_eq!(config.namesrv_addr(), "127.0.0.1:9876");
        assert_eq!(config.namesrv_domain(), "domain");
        assert_eq!(config.namesrv_domain_subgroup(), "subgroup");
    }
}
