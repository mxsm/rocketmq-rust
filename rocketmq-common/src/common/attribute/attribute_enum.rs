/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::collections::HashSet;

use crate::common::attribute::Attribute;
use crate::common::attribute::AttributeTrait;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct EnumAttribute {
    pub(crate) attribute: Attribute,
    pub(crate) universe: HashSet<String>,
    pub(crate) default_value: String,
}

impl AttributeTrait for EnumAttribute {
    fn name(&self) -> String {
        self.attribute.name.clone()
    }

    fn changeable(&self) -> bool {
        self.attribute.changeable
    }

    fn verify(&self, _value: &str) {
        todo!()
    }
}

impl EnumAttribute {
    pub fn get_name(&self) -> &str {
        self.attribute.name.as_str()
    }

    pub fn get_default_value(&self) -> &str {
        &self.default_value
    }

    pub fn get_universe(&self) -> &HashSet<String> {
        &self.universe
    }

    pub fn verify(&self, value: &str) -> bool {
        !self.universe.contains(value)
    }
}
