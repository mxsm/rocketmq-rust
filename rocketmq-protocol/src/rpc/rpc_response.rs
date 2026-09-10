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
    use crate::protocol::header::empty_header::EmptyHeader;
    use crate::protocol::header::get_all_topic_config_response_header::GetAllTopicConfigResponseHeader;

    #[test]
    fn exception_response_uses_zero_without_a_valid_broker_code() {
        let empty = RpcResponse::new_exception(None);
        assert_eq!(empty.code, 0);
        assert!(empty.exception.is_none());
        assert!(empty.header.is_none());
        assert!(empty.body.is_none());

        for error in [
            crate::error::invalid_argument("invalid"),
            Error::new(&BROKER_OPERATION_FAILED),
        ] {
            let descriptor = error.descriptor();
            let response = RpcResponse::new_exception(Some(error));
            assert_eq!(response.code, 0);
            assert_eq!(response.exception.as_ref().unwrap().descriptor(), descriptor);
        }
    }

    #[test]
    fn exception_response_checks_broker_code_bounds_without_truncation() {
        for (code, expected) in [
            (i64::from(i32::MIN), i32::MIN),
            (i64::from(i32::MAX), i32::MAX),
            (i64::from(i32::MIN) - 1, 0),
            (i64::from(i32::MAX) + 1, 0),
        ] {
            let error = Error::new(&BROKER_OPERATION_FAILED)
                .with_context(ErrorContext::new().with_i64(fields::BROKER_CODE, code));
            let response = RpcResponse::new_exception(Some(error));
            assert_eq!(response.code, expected);
            assert_eq!(
                response.exception.as_ref().unwrap().descriptor(),
                &BROKER_OPERATION_FAILED
            );
        }
    }

    #[test]
    fn typed_headers_support_checked_reads_and_mutation_without_changing_body() {
        let header = GetAllTopicConfigResponseHeader {
            total_topic_num: Some(3),
        };
        let mut response = RpcResponse::new(17, Box::new(header), Some(Box::new(vec![1_u8, 2])));
        assert_eq!(response.code, 17);
        assert!(response.exception.is_none());
        assert!(response.get_header::<EmptyHeader>().is_none());
        assert!(response.get_header_mut::<EmptyHeader>().is_none());
        assert_eq!(
            response
                .get_header::<GetAllTopicConfigResponseHeader>()
                .unwrap()
                .total_topic_num,
            Some(3)
        );
        response
            .get_header_mut::<GetAllTopicConfigResponseHeader>()
            .unwrap()
            .total_topic_num = Some(5);
        assert_eq!(
            response
                .get_header::<GetAllTopicConfigResponseHeader>()
                .unwrap()
                .total_topic_num,
            Some(5)
        );
        assert_eq!(
            response.body.as_ref().unwrap().downcast_ref::<Vec<u8>>().unwrap(),
            &vec![1, 2]
        );

        let mut without_header = RpcResponse::new_option(23, Some(Box::new("body")));
        assert_eq!(without_header.code, 23);
        assert!(without_header.exception.is_none());
        assert!(without_header.get_header::<EmptyHeader>().is_none());
        assert!(without_header.get_header_mut::<EmptyHeader>().is_none());
        assert_eq!(
            without_header.body.as_ref().unwrap().downcast_ref::<&str>(),
            Some(&"body")
        );
        assert!(RpcResponse::new_option(0, None).body.is_none());
        assert!(RpcResponse::new(0, Box::new(EmptyHeader::default()), None)
            .body
            .is_none());
    }

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
