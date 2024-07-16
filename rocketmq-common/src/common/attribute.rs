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
pub mod attribute_enum;
pub mod attribute_parser;
pub mod attribute_util;
pub mod cleanup_policy;
pub mod cq_type;
pub mod topic_attributes;
pub mod topic_message_type;

/// `AttributeTrait` defines a common interface for attributes.
///
/// This trait specifies the operations that can be performed on an attribute object.
/// It is designed to be implemented by any struct that represents an attribute, providing
/// a standardized way to interact with attribute data.
pub trait AttributeTrait {
    /// Retrieves the name of the attribute.
    ///
    /// # Returns
    /// A `String` representing the name of the attribute.
    fn name(&self) -> String;

    /// Checks if the attribute is changeable.
    ///
    /// # Returns
    /// A `bool` indicating whether the attribute can be changed after its initial set up.
    fn changeable(&self) -> bool;

    /// Verifies if the provided value is valid for the attribute.
    ///
    /// Implementations should define the criteria for a value to be considered valid.
    ///
    /// # Arguments
    /// * `value` - A string slice representing the value to be verified.
    fn verify(&self, value: &str);
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Attribute {
    pub(crate) name: String,
    pub(crate) changeable: bool,
}
