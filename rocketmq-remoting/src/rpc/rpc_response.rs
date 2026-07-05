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

use std::any::Any;

use rocketmq_error::RocketMQError;
use rocketmq_rust::ArcMut;

use crate::protocol::command_custom_header::CommandCustomHeader;

#[derive(Default)]
pub struct RpcResponse {
    pub code: i32,
    pub header: Option<ArcMut<Box<dyn CommandCustomHeader + Send + Sync + 'static>>>,
    pub body: Option<Box<dyn Any>>,
    pub exception: Option<RocketMQError>,
}

impl RpcResponse {
    pub fn get_header<T>(&self) -> Option<&T>
    where
        T: CommandCustomHeader + Send + Sync + 'static,
    {
        match self.header.as_ref() {
            None => None,
            Some(value) => value.as_ref().as_any().downcast_ref::<T>(),
        }
    }

    pub fn get_header_mut_from_ref<T>(&self) -> Option<&mut T>
    where
        T: CommandCustomHeader + Send + Sync + 'static,
    {
        match self.header.as_ref() {
            None => None,
            Some(value) => value.mut_from_ref().as_any_mut().downcast_mut::<T>(),
        }
    }

    pub fn get_header_mut<T>(&mut self) -> Option<&mut T>
    where
        T: CommandCustomHeader + Send + Sync + 'static,
    {
        match self.header.as_mut() {
            None => None,
            Some(value) => value.as_mut().as_any_mut().downcast_mut::<T>(),
        }
    }

    pub fn new_exception(exception: Option<RocketMQError>) -> Self {
        Self {
            code: exception.as_ref().map_or(0, |e| match e {
                RocketMQError::BrokerOperationFailed { code, .. } => *code,
                _ => 0,
            }),
            header: None,
            body: None,
            exception,
        }
    }

    pub fn new(
        code: i32,
        header: Box<dyn CommandCustomHeader + Send + Sync + 'static>,
        body: Option<Box<dyn Any>>,
    ) -> Self {
        Self {
            code,
            header: Some(ArcMut::new(header)),
            body,
            exception: None,
        }
    }

    pub fn new_option(code: i32, body: Option<Box<dyn Any>>) -> Self {
        Self {
            code,
            header: None,
            body,
            exception: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_error::RocketMQError;

    use super::*;

    #[test]
    fn new_exception_accepts_typed_broker_operation_error() {
        let response = RpcResponse::new_exception(Some(RocketMQError::broker_operation_failed(
            "RPC",
            206,
            "broker rejected",
        )));

        assert_eq!(response.code, 206);
        assert!(matches!(
            response.exception,
            Some(RocketMQError::BrokerOperationFailed { code: 206, .. })
        ));
    }

    #[test]
    fn new_exception_uses_zero_code_for_non_broker_error() {
        let response = RpcResponse::new_exception(Some(RocketMQError::response_process_failed(
            "rpc_response",
            "local failure",
        )));

        assert_eq!(response.code, 0);
        assert!(matches!(
            response.exception,
            Some(RocketMQError::ResponseProcessFailed { .. })
        ));
    }
}
