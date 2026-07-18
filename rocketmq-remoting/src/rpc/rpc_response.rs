// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::any::Any;

use rocketmq_error::RocketMQError;
use rocketmq_protocol::rpc::rpc_response::RpcResponse as CanonicalRpcResponse;

use crate::protocol::command_custom_header::CommandCustomHeader;

#[derive(Default)]
pub struct RpcResponse {
    pub code: i32,
    pub header: Option<Box<dyn CommandCustomHeader + Send + Sync + 'static>>,
    pub body: Option<Box<dyn Any>>,
    pub exception: Option<RocketMQError>,
}

impl RpcResponse {
    pub fn get_header<T>(&self) -> Option<&T>
    where
        T: CommandCustomHeader + Send + Sync + 'static,
    {
        self.header.as_ref()?.as_any().downcast_ref::<T>()
    }

    pub fn get_header_mut<T>(&mut self) -> Option<&mut T>
    where
        T: CommandCustomHeader + Send + Sync + 'static,
    {
        self.header.as_mut()?.as_any_mut().downcast_mut::<T>()
    }

    pub fn new_exception(exception: Option<RocketMQError>) -> Self {
        Self {
            code: exception.as_ref().map_or(0, |error| match error {
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
            header: Some(header),
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

    pub fn try_into_canonical(self) -> Result<CanonicalRpcResponse, Self> {
        let Self {
            code,
            header,
            body,
            exception,
        } = self;
        Ok(CanonicalRpcResponse {
            code,
            header,
            body,
            exception,
        })
    }

    pub fn from_canonical(value: CanonicalRpcResponse) -> Self {
        match value.header {
            Some(header) => {
                let mut response = Self::new(value.code, header, value.body);
                response.exception = value.exception;
                response
            }
            None => Self {
                code: value.code,
                header: None,
                body: value.body,
                exception: value.exception,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::RpcResponse;
    use crate::protocol::header::client_request_header::GetRouteInfoRequestHeader;

    #[test]
    fn header_mutation_requires_exclusive_response_access() {
        let mut response = RpcResponse::new(0, Box::new(GetRouteInfoRequestHeader::new("before", Some(true))), None);

        response
            .get_header_mut::<GetRouteInfoRequestHeader>()
            .expect("typed header must be present")
            .topic = CheetahString::from_static_str("after");

        assert_eq!(
            response
                .get_header::<GetRouteInfoRequestHeader>()
                .expect("typed header must remain present")
                .topic,
            CheetahString::from_static_str("after")
        );
    }

    #[test]
    fn canonical_conversion_preserves_owned_header() {
        let response = RpcResponse::new(17, Box::new(GetRouteInfoRequestHeader::new("topic", None)), None);

        let canonical = match response.try_into_canonical() {
            Ok(response) => response,
            Err(_) => panic!("owned response conversion must not fail"),
        };

        assert_eq!(canonical.code, 17);
        assert_eq!(
            canonical
                .get_header::<GetRouteInfoRequestHeader>()
                .expect("typed header must survive conversion")
                .topic,
            CheetahString::from_static_str("topic")
        );
    }
}
