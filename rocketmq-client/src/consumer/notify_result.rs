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

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NotifyResult {
    has_msg: bool,
    polling_full: bool,
}

impl NotifyResult {
    pub fn new(has_msg: bool, polling_full: bool) -> Self {
        Self { has_msg, polling_full }
    }

    #[inline]
    pub fn has_msg(&self) -> bool {
        self.has_msg
    }

    #[inline]
    pub fn is_has_msg(&self) -> bool {
        self.has_msg
    }

    #[inline]
    pub fn set_has_msg(&mut self, has_msg: bool) {
        self.has_msg = has_msg;
    }

    #[inline]
    pub fn polling_full(&self) -> bool {
        self.polling_full
    }

    #[inline]
    pub fn is_polling_full(&self) -> bool {
        self.polling_full
    }

    #[inline]
    pub fn set_polling_full(&mut self, polling_full: bool) {
        self.polling_full = polling_full;
    }
}

impl fmt::Display for NotifyResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "NotifyResult{{hasMsg={}, pollingFull={}}}",
            self.has_msg, self.polling_full
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notify_result_getters_setters_match_java_shape() {
        let mut result = NotifyResult::default();
        assert!(!result.is_has_msg());
        assert!(!result.is_polling_full());

        result.set_has_msg(true);
        result.set_polling_full(true);

        assert!(result.has_msg());
        assert!(result.polling_full());
        assert_eq!(result.to_string(), "NotifyResult{hasMsg=true, pollingFull=true}");
    }
}
