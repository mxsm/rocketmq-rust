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

pub struct JavaStringHasher;

impl JavaStringHasher {
    pub fn hash_str(s: &str) -> i32 {
        let mut state = 0i32;
        if !s.is_empty() {
            for c in s.chars() {
                state = state.wrapping_mul(31).wrapping_add(c as i32);
            }
        }
        state
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hash_str_with_non_empty_string() {
        let result = JavaStringHasher::hash_str("hello");
        assert_eq!(result, 99162322);
    }

    #[test]
    fn hash_str_with_empty_string() {
        let result = JavaStringHasher::hash_str("");
        assert_eq!(result, 0);
    }

    #[test]
    fn hash_str_with_special_characters() {
        let result = JavaStringHasher::hash_str("!@#");
        assert_eq!(result, 33732);
    }

    #[test]
    fn hash_str_with_long_string() {
        let result = JavaStringHasher::hash_str("a".repeat(1000).as_str());
        assert_eq!(result, 904019584);
    }
}
