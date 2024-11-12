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
use cheetah_string::CheetahString;

pub struct StringUtils;

impl StringUtils {
    #[inline]
    pub fn is_not_empty_str(s: Option<&str>) -> bool {
        s.map_or(false, |s| !s.is_empty())
    }

    #[inline]
    pub fn is_not_empty_string(s: Option<&String>) -> bool {
        s.map_or(false, |s| !s.is_empty())
    }

    #[inline]
    pub fn is_not_empty_ch_string(s: Option<&CheetahString>) -> bool {
        s.map_or(false, |s| !s.is_empty())
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
}
