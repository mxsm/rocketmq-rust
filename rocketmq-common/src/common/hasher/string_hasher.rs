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
use std::hash::Hasher;

//Compatible with Java String's hash code
pub struct JavaStringHasher {
    state: i32,
}

impl Default for JavaStringHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl JavaStringHasher {
    pub fn new() -> Self {
        JavaStringHasher { state: 0 }
    }

    pub fn hash_str(&mut self, s: &str) -> i32 {
        if self.state == 0 && !s.is_empty() {
            for c in s.chars() {
                self.state = self.state.wrapping_mul(31).wrapping_add(c as i32);
            }
        }
        self.state
    }
}

impl Hasher for JavaStringHasher {
    fn finish(&self) -> u64 {
        self.state as u64
    }

    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.state = self.state.wrapping_mul(31).wrapping_add(byte as i32);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_java_string_hasher() {
        let mut hasher = JavaStringHasher::new();
        let i = hasher.hash_str("hello world");
        assert_eq!(i, 1794106052);
    }
}
