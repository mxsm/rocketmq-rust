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
        self.header.as_ref()?.as_ref().as_any().downcast_ref::<T>()
    }

    pub fn get_header_mut<T>(&mut self) -> Option<&mut T>
    where
        T: CommandCustomHeader + Send + Sync + 'static,
    {
        match self.header.as_mut() {
            Some(value) if value.strong_count() == 1 => value.as_mut().as_any_mut().downcast_mut::<T>(),
            _ => None,
        }
    }

    /// Compatibility facade for the removed shared-reference mutation escape.
    ///
    /// Returning a mutable reference from `&self` cannot be made sound. Callers
    /// must migrate to [`Self::get_header_mut`], which requires exclusive access.
    #[deprecated(note = "use get_header_mut; shared-reference mutation is no longer supported")]
    pub fn get_header_mut_from_ref<T>(&self) -> Option<&mut T>
    where
        T: CommandCustomHeader + Send + Sync + 'static,
    {
        None
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

    pub fn try_into_canonical(self) -> Result<CanonicalRpcResponse, Self> {
        let Self {
            code,
            header,
            body,
            exception,
        } = self;
        let header = match header {
            Some(header) => match header.try_unwrap() {
                Ok(header) => Some(header),
                Err(header) => {
                    return Err(Self {
                        code,
                        header: Some(header),
                        body,
                        exception,
                    });
                }
            },
            None => None,
        };
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
