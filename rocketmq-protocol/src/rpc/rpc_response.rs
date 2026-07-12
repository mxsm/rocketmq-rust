// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::any::Any;

use rocketmq_error::RocketMQError;

use crate::protocol::CommandCustomHeader;

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
}
