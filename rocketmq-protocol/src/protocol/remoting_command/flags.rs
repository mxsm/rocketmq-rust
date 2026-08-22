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

use super::RemotingCommand;
use super::RemotingCommandType;
use super::SerializeType;

impl RemotingCommand {
    pub(crate) const RPC_ONEWAY: i32 = 1;
    pub(crate) const RPC_TYPE: i32 = 0;

    #[inline]
    pub fn set_flag(mut self, flag: i32) -> Self {
        self.flag = flag;
        self
    }

    #[inline]
    pub fn mark_response_type(mut self) -> Self {
        let mark = 1 << Self::RPC_TYPE;
        self.flag |= mark;
        self
    }

    #[inline]
    pub fn mark_response_type_ref(&mut self) {
        let mark = 1 << Self::RPC_TYPE;
        self.flag |= mark;
    }

    #[inline]
    pub fn mark_oneway_rpc(mut self) -> Self {
        let mark = 1 << Self::RPC_ONEWAY;
        self.flag |= mark;
        self
    }

    #[inline]
    pub fn mark_oneway_rpc_ref(&mut self) {
        let mark = 1 << Self::RPC_ONEWAY;
        self.flag |= mark;
    }

    #[inline]
    pub fn get_serialize_type(&self) -> SerializeType {
        self.serialize_type
    }

    #[inline]
    pub fn flag(&self) -> i32 {
        self.flag
    }

    #[inline]
    pub fn is_response_type(&self) -> bool {
        let bits = 1 << Self::RPC_TYPE;
        (self.flag & bits) == bits
    }

    #[inline]
    pub fn is_oneway_rpc(&self) -> bool {
        let bits = 1 << Self::RPC_ONEWAY;
        (self.flag & bits) == bits
    }

    pub fn get_type(&self) -> RemotingCommandType {
        if self.is_response_type() {
            RemotingCommandType::RESPONSE
        } else {
            RemotingCommandType::REQUEST
        }
    }
}
