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

use super::*;

pub struct TransactionClient<'a> {
    api: &'a MQClientAPIImpl,
}

impl TransactionClient<'_> {
    pub async fn end_transaction(
        &self,
        addr: &CheetahString,
        request_header: EndTransactionRequestHeader,
        remark: CheetahString,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.api
            .end_transaction_oneway(addr, request_header, remark, timeout_millis)
            .await
    }
}

impl MQClientAPIImpl {
    #[must_use]
    pub fn transaction_client(&self) -> TransactionClient<'_> {
        TransactionClient { api: self }
    }
}

impl MQClientAPIImpl {
    pub async fn end_transaction_oneway(
        &self,
        addr: &CheetahString,
        request_header: EndTransactionRequestHeader,
        remark: CheetahString,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request =
            RemotingCommand::create_request_command(RequestCode::EndTransaction, request_header).set_remark(remark);

        self.remoting_client
            .invoke_request_oneway(addr, request, timeout_millis)
            .await;
        Ok(())
    }
}
