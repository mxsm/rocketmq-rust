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
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::body::cm_result::CMResult;

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumeMessageDirectlyResult {
    order: bool,
    auto_commit: bool,
    consume_result: CMResult,
    remark: String,
    spent_time_mills: u64,
}

impl ConsumeMessageDirectlyResult {
    pub fn new(
        order: bool,
        auto_commit: bool,
        consume_result: CMResult,
        remark: String,
        spent_time_mills: u64,
    ) -> Self {
        Self {
            order,
            auto_commit,
            consume_result,
            remark,
            spent_time_mills,
        }
    }

    pub fn order(&self) -> bool {
        self.order
    }

    pub fn set_order(&mut self, order: bool) {
        self.order = order;
    }

    pub fn auto_commit(&self) -> bool {
        self.auto_commit
    }

    pub fn set_auto_commit(&mut self, auto_commit: bool) {
        self.auto_commit = auto_commit;
    }

    pub fn consume_result(&self) -> &CMResult {
        &self.consume_result
    }

    pub fn set_consume_result(&mut self, consume_result: CMResult) {
        self.consume_result = consume_result;
    }

    pub fn remark(&self) -> &str {
        &self.remark
    }

    pub fn set_remark(&mut self, remark: String) {
        self.remark = remark;
    }

    pub fn spent_time_mills(&self) -> u64 {
        self.spent_time_mills
    }

    pub fn set_spent_time_mills(&mut self, spent_time_mills: u64) {
        self.spent_time_mills = spent_time_mills;
    }
}
