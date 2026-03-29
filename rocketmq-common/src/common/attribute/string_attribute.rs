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

use crate::common::attribute::Attribute;
use crate::common::attribute::AttributeBase;

#[derive(Debug, Clone)]
pub struct StringAttribute {
    attribute: AttributeBase,
}

impl StringAttribute {
    pub fn new(name: CheetahString, changeable: bool) -> Self {
        Self {
            attribute: AttributeBase::new(name, changeable),
        }
    }
}

impl Attribute for StringAttribute {
    fn verify(&self, _value: &str) -> Result<(), String> {
        Ok(())
    }

    fn name(&self) -> &CheetahString {
        self.attribute.name()
    }

    fn is_changeable(&self) -> bool {
        self.attribute.is_changeable()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_attribute_accepts_empty_and_non_empty_values() {
        let attribute = StringAttribute::new(CheetahString::from_static_str("lite.bind.topic"), true);

        assert!(attribute.verify("").is_ok());
        assert!(attribute.verify("parent-topic").is_ok());
    }
}
