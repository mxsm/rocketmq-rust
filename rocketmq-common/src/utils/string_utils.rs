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

use std::collections::BTreeMap;
use std::collections::HashMap;

use cheetah_string::CheetahString;

pub struct StringUtils;

impl StringUtils {
    #[inline]
    pub fn is_not_empty_str(s: Option<&str>) -> bool {
        s.is_some_and(|s| !s.is_empty())
    }

    #[inline]
    pub fn is_not_empty_string(s: Option<&String>) -> bool {
        s.is_some_and(|s| !s.is_empty())
    }

    #[inline]
    pub fn is_not_empty_ch_string(s: Option<&CheetahString>) -> bool {
        s.is_some_and(|s| !s.is_empty())
    }

    pub fn parse_delay_level(level_string: &str) -> Result<(BTreeMap<i32, i64>, i32), String> {
        let time_unit_table = HashMap::from([
            ('s', 1000),
            ('m', 1000 * 60),
            ('h', 1000 * 60 * 60),
            ('d', 1000 * 60 * 60 * 24),
        ]);

        let mut delay_level_table = BTreeMap::new();

        let level_array: Vec<&str> = level_string.split(' ').collect();
        let mut max_delay_level = 0;

        for (i, value) in level_array.iter().enumerate() {
            // let ch = value.chars().last().unwrap().to_string();
            let ch = match value.chars().last() {
                None => {
                    return Err("Empty time unit".to_string());
                }
                Some(value) => value,
            };
            let tu = *time_unit_table.get(&ch).ok_or(format!("Unknown time unit: {ch}"))?;

            let level = i as i32 + 1;
            if level > max_delay_level {
                max_delay_level = level;
            }

            let num_str = &value[0..value.len() - 1];
            let num = num_str.parse::<i64>().map_err(|e| e.to_string())?;
            let delay_time_millis = tu * num;
            delay_level_table.insert(level, delay_time_millis);
        }

        Ok((delay_level_table, max_delay_level))
    }
}

#[cfg(test)]
mod tests {
    use super::StringUtils;

    #[test]
    fn is_not_empty_str_with_some_non_empty_str() {
        assert!(StringUtils::is_not_empty_str(Some("hello")));
    }

    #[test]
    fn is_not_empty_str_with_some_empty_str() {
        assert!(!StringUtils::is_not_empty_str(Some("")));
    }

    #[test]
    fn is_not_empty_str_with_none() {
        assert!(!StringUtils::is_not_empty_str(None));
    }

    #[test]
    fn is_not_empty_string_with_some_non_empty_string() {
        assert!(StringUtils::is_not_empty_string(Some(&"hello".to_string())));
    }

    #[test]
    fn is_not_empty_string_with_some_empty_string() {
        assert!(!StringUtils::is_not_empty_string(Some(&"".to_string())));
    }

    #[test]
    fn is_not_empty_string_with_none() {
        assert!(!StringUtils::is_not_empty_string(None));
    }

    #[test]
    fn parse_delay_level_with_valid_input() {
        let result = StringUtils::parse_delay_level("1s 2m 3h 4d").unwrap();
        let (delay_level_table, max_delay_level) = result;
        assert_eq!(delay_level_table[&1], 1000);
        assert_eq!(delay_level_table[&2], 2 * 60 * 1000);
        assert_eq!(delay_level_table[&3], 3 * 60 * 60 * 1000);
        assert_eq!(delay_level_table[&4], 4 * 24 * 60 * 60 * 1000);
        assert_eq!(max_delay_level, 4);
    }

    #[test]
    fn parse_delay_level_with_unknown_time_unit() {
        let result = StringUtils::parse_delay_level("1x");
        assert!(result.is_err());
        assert_eq!(result.err().unwrap(), "Unknown time unit: x");
    }

    #[test]
    fn parse_delay_level_with_invalid_number() {
        let result = StringUtils::parse_delay_level("1s 2m 3h 4d 5z");
        assert!(result.is_err());
        assert_eq!(result.err().unwrap(), "Unknown time unit: z");
    }

    #[test]
    fn parse_delay_level_with_empty_string() {
        let result = StringUtils::parse_delay_level("");
        assert!(result.is_err());
    }
}
