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

use std::hash::Hash;
use std::hash::Hasher;

use rocketmq_remoting::protocol::LanguageCode;
use serde::Deserialize;
use serde::Serialize;

/// Producer attributes for metrics tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerAttr {
    /// Programming language of the producer client
    pub language: LanguageCode,

    /// Client version
    pub version: i32,
}

impl ProducerAttr {
    /// Create new producer attributes
    ///
    /// # Arguments
    /// * `language` - Programming language of the producer client
    /// * `version` - Client version
    pub fn new(language: LanguageCode, version: i32) -> Self {
        Self { language, version }
    }
}

impl PartialEq for ProducerAttr {
    fn eq(&self, other: &Self) -> bool {
        self.version == other.version && self.language == other.language
    }
}

impl Eq for ProducerAttr {}

impl Hash for ProducerAttr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.language.hash(state);
        self.version.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_producer_attr_equality() {
        let attr1 = ProducerAttr::new(LanguageCode::RUST, 1);
        let attr2 = ProducerAttr::new(LanguageCode::RUST, 1);
        let attr3 = ProducerAttr::new(LanguageCode::JAVA, 1);
        let attr4 = ProducerAttr::new(LanguageCode::RUST, 2);

        assert_eq!(attr1, attr2);
        assert_ne!(attr1, attr3);
        assert_ne!(attr1, attr4);
    }

    #[test]
    fn test_producer_attr_hash() {
        use std::collections::hash_map::DefaultHasher;

        let attr1 = ProducerAttr::new(LanguageCode::RUST, 1);
        let attr2 = ProducerAttr::new(LanguageCode::RUST, 1);

        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();

        attr1.hash(&mut hasher1);
        attr2.hash(&mut hasher2);

        assert_eq!(hasher1.finish(), hasher2.finish());
    }
}
