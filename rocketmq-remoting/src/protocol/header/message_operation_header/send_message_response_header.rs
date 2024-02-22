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

use rocketmq_macros::{RemotingSerializable, RequestHeaderCodec};
use serde::{Deserialize, Serialize};

use crate::protocol::FastCodesHeader;

#[derive(Debug, Serialize, Deserialize, Default, RemotingSerializable, RequestHeaderCodec)]
#[serde(rename_all = "camelCase")]
pub struct SendMessageResponseHeader {
    msg_id: String,
    queue_id: i32,
    queue_offset: i64,
    transaction_id: Option<String>,
    batch_uniq_id: Option<String>,
}

impl SendMessageResponseHeader {
    pub fn new(
        msg_id: String,
        queue_id: i32,
        queue_offset: i64,
        transaction_id: Option<String>,
        batch_uniq_id: Option<String>,
    ) -> Self {
        SendMessageResponseHeader {
            msg_id,
            queue_id,
            queue_offset,
            transaction_id,
            batch_uniq_id,
        }
    }

    pub fn msg_id(&self) -> &str {
        &self.msg_id
    }

    pub fn queue_id(&self) -> i32 {
        self.queue_id
    }

    pub fn queue_offset(&self) -> i64 {
        self.queue_offset
    }

    pub fn transaction_id(&self) -> Option<&str> {
        self.transaction_id.as_deref()
    }

    pub fn batch_uniq_id(&self) -> Option<&str> {
        self.batch_uniq_id.as_deref()
    }
}

impl FastCodesHeader for SendMessageResponseHeader {
    fn encode_fast(&mut self, out: &mut bytes::BytesMut) {
        self.write_if_not_null(out, "msgId", self.msg_id.to_string().as_str());
        self.write_if_not_null(out, "queueId", self.queue_id.to_string().as_str());
        self.write_if_not_null(out, "queueOffset", self.queue_offset.to_string().as_str());
        self.write_if_not_null(
            out,
            "transactionId",
            self.transaction_id.clone().as_deref().unwrap(),
        );
        self.write_if_not_null(
            out,
            "batchUniqId",
            self.batch_uniq_id.clone().as_deref().unwrap(),
        );
    }

    fn decode_fast(&mut self, fields: &HashMap<String, String>) {
        if let Some(str) = fields.get("msgId") {
            self.msg_id = str.clone();
        }

        if let Some(str) = fields.get("queueId") {
            self.queue_id = str.parse::<i32>().unwrap_or_default();
        }

        if let Some(str) = fields.get("queueOffset") {
            self.queue_offset = str.parse::<i64>().unwrap_or_default();
        }

        if let Some(str) = fields.get("transactionId") {
            self.transaction_id = Some(str.clone());
        }

        if let Some(str) = fields.get("batchUniqId") {
            self.batch_uniq_id = Some(str.clone());
        }
    }
}
