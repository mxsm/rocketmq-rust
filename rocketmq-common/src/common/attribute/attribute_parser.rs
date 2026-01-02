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

use std::collections::HashMap;
use std::string::ToString;

const ATTR_ARRAY_SEPARATOR_COMMA: &str = ",";
const ATTR_KEY_VALUE_EQUAL_SIGN: &str = "=";
const ATTR_ADD_PLUS_SIGN: &str = "+";
const ATTR_DELETE_MINUS_SIGN: &str = "-";

#[derive(Debug)]
pub struct AttributeParser;

impl AttributeParser {
    pub fn parse_to_map(attributes_modification: &str) -> Result<HashMap<String, String>, String> {
        if attributes_modification.is_empty() {
            return Ok(HashMap::new());
        }

        let mut attributes = HashMap::new();
        let kvs: Vec<&str> = attributes_modification.split(ATTR_ARRAY_SEPARATOR_COMMA).collect();
        for kv in kvs {
            let mut key = String::new();
            let mut value = String::new();
            if kv.contains(ATTR_KEY_VALUE_EQUAL_SIGN) {
                let splits: Vec<&str> = kv.split(ATTR_KEY_VALUE_EQUAL_SIGN).collect();
                key.push_str(splits[0]);
                value.push_str(splits[1]);
                if !key.contains(ATTR_ADD_PLUS_SIGN) {
                    return Err(format!("add/alter attribute format is wrong: {key}"));
                }
            } else {
                key.push_str(kv);
                if !key.contains(ATTR_DELETE_MINUS_SIGN) {
                    return Err(format!("delete attribute format is wrong: {key}",));
                }
            }
            if attributes.insert(key.clone(), value).is_some() {
                return Err(format!("key duplication: {key}",));
            }
        }
        Ok(attributes)
    }

    pub fn parse_to_string(attributes: &HashMap<String, String>) -> String {
        if attributes.is_empty() {
            return String::new();
        }

        let mut kvs: Vec<String> = Vec::new();
        for (key, value) in attributes {
            if value.is_empty() {
                kvs.push(key.clone());
            } else {
                kvs.push(format!("{key}{ATTR_KEY_VALUE_EQUAL_SIGN}{value}"));
            }
        }
        kvs.join(ATTR_ARRAY_SEPARATOR_COMMA)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_to_map_empty_string() {
        let result = AttributeParser::parse_to_map("");
        assert_eq!(result.unwrap(), HashMap::new());
    }

    #[test]
    fn test_parse_to_map_valid_attributes() {
        let input = "+key1=value1,+key2=value2,-key3";
        let mut expected = HashMap::new();
        expected.insert("+key1".to_string(), "value1".to_string());
        expected.insert("+key2".to_string(), "value2".to_string());
        expected.insert("-key3".to_string(), "".to_string());

        let result = AttributeParser::parse_to_map(input);
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn test_parse_to_map_add_attribute_format_error() {
        let input = "key1=value1,+key2=value2";
        let result = AttributeParser::parse_to_map(input);
        assert_eq!(
            result.unwrap_err(),
            "add/alter attribute format is wrong: key1".to_string()
        );
    }

    #[test]
    fn test_parse_to_map_delete_attribute_format_error() {
        let input = "+key1=value1,key2";
        let result = AttributeParser::parse_to_map(input);
        assert_eq!(
            result.unwrap_err(),
            "delete attribute format is wrong: key2".to_string()
        );
    }

    #[test]
    fn test_parse_to_map_key_duplication_error() {
        let input = "+key1=value1,+key1=value2";
        let result = AttributeParser::parse_to_map(input);
        assert_eq!(result.unwrap_err(), "key duplication: +key1".to_string());
    }

    #[test]
    fn test_parse_to_string_empty_map() {
        let attributes = HashMap::new();
        let result = AttributeParser::parse_to_string(&attributes);
        assert_eq!(result, "");
    }

    // #[test]
    // fn test_parse_to_string_valid_map() {
    //     let mut attributes = HashMap::new();
    //     attributes.insert("+key1".to_string(), "value1".to_string());
    //     attributes.insert("+key2".to_string(), "value2".to_string());
    //     attributes.insert("-key3".to_string(), "".to_string());

    //     let result = AttributeParser::parse_to_string(&attributes);
    //     assert!(
    //         result == "+key1=value1,+key2=value2,-key3"
    //             || result == "+key2=value2,+key1=value1,-key3"
    //     );
    // }
}
