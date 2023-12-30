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

use std::{collections::HashMap, fmt::Debug};

pub trait CommandCustomHeader {
    /// Checks the fields of the implementing type.  
    ///  
    /// Returns a `Result` indicating whether the fields are valid or not.  
    /// If the fields are valid, the `Ok` variant is returned with an empty `()` value.  
    /// If the fields are invalid, an `Err` variant is returned with an associated `Error` value.  
    fn check_fields(&self) -> anyhow::Result<(), anyhow::Error>;

    /// Converts the implementing type to a map.  
    ///  
    /// Returns an `Option` that contains a `HashMap` of string keys and string values,  
    /// representing the implementing type's fields.  
    /// If the conversion is successful, a non-empty map is returned.  
    /// If the conversion fails, `None` is returned.  
    fn to_map(&self) -> Option<HashMap<String, String>>;
}

pub trait FromMap {
    type Target;
    /// Converts the implementing type from a map.
    ///
    /// Returns an instance of `Self::Target` that is created from the provided map.
    fn from(map: &HashMap<String, String>) -> Option<Self::Target>;
}
