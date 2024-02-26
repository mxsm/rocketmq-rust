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

pub mod attribute_enum;
pub mod cq_type;
pub mod topic_attributes;
pub mod topic_message_type;

pub trait Attribute {
    fn verify(&self, value: &str);
    fn get_name(&self) -> &str;
    fn is_changeable(&self) -> bool;
}

pub struct EnumAttribute {
    name: String,
    changeable: bool,
    universe: HashSet<String>,
    default_value: String,
}

impl EnumAttribute {
    pub fn new(
        name: impl Into<String>,
        changeable: bool,
        universe: HashSet<String>,
        default_value: impl Into<String>,
    ) -> Self {
        EnumAttribute {
            name: name.into(),
            changeable,
            universe,
            default_value: default_value.into(),
        }
    }

    pub fn get_default_value(&self) -> &str {
        &self.default_value
    }
}

impl Attribute for EnumAttribute {
    fn verify(&self, value: &str) {
        if !self.universe.contains(value) {
            panic!("value is not in set: {:?}", self.universe);
        }
    }

    fn get_name(&self) -> &str {
        &self.name
    }

    fn is_changeable(&self) -> bool {
        self.changeable
    }
}
