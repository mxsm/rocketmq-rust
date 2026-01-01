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

use std::fmt::Display;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::body::cm_result::CMResult;

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumeMessageDirectlyResult {
    order: bool,
    auto_commit: bool,
    consume_result: Option<CMResult>,
    remark: Option<CheetahString>,
    spent_time_mills: u64,
}

impl Default for ConsumeMessageDirectlyResult {
    #[inline]
    fn default() -> Self {
        Self {
            order: false,
            auto_commit: true,
            consume_result: None,
            remark: None,
            spent_time_mills: 0,
        }
    }
}

impl Display for ConsumeMessageDirectlyResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ConsumeMessageDirectlyResult [order={}, auto_commit={}, consume_result={:?}, remark={:?}, \
             spent_time_mills={}]",
            self.order, self.auto_commit, self.consume_result, self.remark, self.spent_time_mills
        )
    }
}

impl ConsumeMessageDirectlyResult {
    pub fn new(
        order: bool,
        auto_commit: bool,
        consume_result: CMResult,
        remark: CheetahString,
        spent_time_mills: u64,
    ) -> Self {
        Self {
            order,
            auto_commit,
            consume_result: Some(consume_result),
            remark: Some(remark),
            spent_time_mills,
        }
    }

    #[inline]
    pub fn order(&self) -> bool {
        self.order
    }

    #[inline]
    pub fn set_order(&mut self, order: bool) {
        self.order = order;
    }

    #[inline]
    pub fn auto_commit(&self) -> bool {
        self.auto_commit
    }

    #[inline]
    pub fn set_auto_commit(&mut self, auto_commit: bool) {
        self.auto_commit = auto_commit;
    }

    #[inline]
    pub fn consume_result(&self) -> Option<&CMResult> {
        self.consume_result.as_ref()
    }

    #[inline]
    pub fn set_consume_result(&mut self, consume_result: CMResult) {
        self.consume_result = Some(consume_result);
    }

    #[inline]
    pub fn remark(&self) -> Option<&CheetahString> {
        self.remark.as_ref()
    }

    #[inline]
    pub fn set_remark(&mut self, remark: CheetahString) {
        self.remark = Some(remark);
    }

    #[inline]
    pub fn spent_time_mills(&self) -> u64 {
        self.spent_time_mills
    }

    #[inline]
    pub fn set_spent_time_mills(&mut self, spent_time_mills: u64) {
        self.spent_time_mills = spent_time_mills;
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;
    use crate::protocol::body::cm_result::CMResult;

    #[test]
    fn consume_message_directly_result_default_initializes_correctly() {
        let result = ConsumeMessageDirectlyResult::default();
        assert!(!result.order());
        assert!(result.auto_commit());
        assert!(result.consume_result().is_none());
        assert!(result.remark().is_none());
        assert_eq!(result.spent_time_mills(), 0);
    }

    #[test]
    fn consume_message_directly_result_new_initializes_correctly() {
        let consume_result = CMResult::default();
        let remark = CheetahString::from_static_str("test remark");
        let result = ConsumeMessageDirectlyResult::new(true, false, consume_result, remark.clone(), 12345);
        assert!(result.order());
        assert!(!result.auto_commit());
        assert_eq!(result.consume_result().unwrap(), &consume_result);
        assert_eq!(result.remark(), Some(&remark));
        assert_eq!(result.spent_time_mills(), 12345);
    }

    #[test]
    fn consume_message_directly_result_setters_work_correctly() {
        let mut result = ConsumeMessageDirectlyResult::default();
        result.set_order(true);
        result.set_auto_commit(false);
        let consume_result = CMResult::default();
        result.set_consume_result(consume_result);
        let remark = CheetahString::from_static_str("updated remark");
        result.set_remark(remark.clone());
        result.set_spent_time_mills(67890);

        assert!(result.order());
        assert!(!result.auto_commit());
        assert_eq!(result.consume_result().unwrap(), &consume_result);
        assert_eq!(result.remark(), Some(&remark));
        assert_eq!(result.spent_time_mills(), 67890);
    }

    #[test]
    fn consume_message_directly_result_display_formats_correctly() {
        let consume_result = CMResult::default();
        let remark = CheetahString::from_static_str("test remark");
        let result = ConsumeMessageDirectlyResult::new(true, false, consume_result, remark.clone(), 12345);
        let display = format!("{}", result);
        let expected = format!(
            "ConsumeMessageDirectlyResult [order=true, auto_commit=false, consume_result={:?}, remark={:?}, \
             spent_time_mills=12345]",
            Some(consume_result),
            Some(remark)
        );
        assert_eq!(display, expected);
    }
}
