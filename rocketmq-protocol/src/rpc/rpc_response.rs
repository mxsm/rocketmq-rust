// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::any::Any;

use rocketmq_error::Error;
use rocketmq_error::ViewValueRef;
use rocketmq_error::BROKER_OPERATION_FAILED;

use crate::protocol::CommandCustomHeader;

#[derive(Default)]
pub struct RpcResponse {
    pub code: i32,
    pub header: Option<Box<dyn CommandCustomHeader + Send + Sync + 'static>>,
    pub body: Option<Box<dyn Any>>,
    pub exception: Option<Error>,
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

    pub fn new_exception(exception: Option<Error>) -> Self {
        Self {
            code: exception.as_ref().map_or(0, broker_response_code),
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

fn broker_response_code(error: &Error) -> i32 {
    if error.descriptor() != &BROKER_OPERATION_FAILED {
        return 0;
    }

    error
        .diagnostic_view()
        .ok()
        .and_then(|view| {
            view.fields()
                .find(|field| field.name() == rocketmq_error::fields::BROKER_CODE.schema().name())
                .and_then(|field| match field.value() {
                    ViewValueRef::I64(code) => i32::try_from(code).ok(),
                    _ => None,
                })
        })
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use rocketmq_error::fields;
    use rocketmq_error::ErrorContext;

    use super::*;

    #[test]
    fn exception_response_retains_canonical_broker_code() {
        let error = Error::new(&BROKER_OPERATION_FAILED).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, "test")
                .with_i64(fields::BROKER_CODE, 207)
                .with_secret_presence(fields::MESSAGE_PRESENT),
        );

        let response = RpcResponse::new_exception(Some(error));

        assert_eq!(response.code, 207);
        assert_eq!(
            response.exception.as_ref().map(Error::descriptor),
            Some(&BROKER_OPERATION_FAILED)
        );
    }
}
