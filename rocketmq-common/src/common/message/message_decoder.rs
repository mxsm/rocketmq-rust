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
use std::collections::HashMap;

pub const NAME_VALUE_SEPARATOR: char = '\u{0001}';
pub const PROPERTY_SEPARATOR: char = '\u{0002}';

pub fn string_to_message_properties(properties: Option<&String>) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if let Some(properties) = properties {
        let mut index = 0;
        let len = properties.len();
        while index < len {
            let new_index = properties[index..]
                .find(PROPERTY_SEPARATOR)
                .map_or(len, |i| index + i);
            if new_index - index >= 3 {
                if let Some(kv_sep_index) = properties[index..new_index].find(NAME_VALUE_SEPARATOR)
                {
                    let kv_sep_index = index + kv_sep_index;
                    if kv_sep_index > index && kv_sep_index < new_index - 1 {
                        let k = &properties[index..kv_sep_index];
                        let v = &properties[kv_sep_index + 1..new_index];
                        map.insert(k.to_string(), v.to_string());
                    }
                }
            }
            index = new_index + 1;
        }
    }
    map
}

pub fn message_properties_to_string(properties: &HashMap<String, String>) -> String {
    let mut len = 0;
    for (name, value) in properties.iter() {
        len += name.len();

        len += value.len();
        len += 2; // separator
    }

    let mut sb = String::with_capacity(len);
    for (name, value) in properties.iter() {
        sb.push_str(name);
        sb.push(NAME_VALUE_SEPARATOR);

        sb.push_str(value);
        sb.push(PROPERTY_SEPARATOR);
    }
    sb
}
