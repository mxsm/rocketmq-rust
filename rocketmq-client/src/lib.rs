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

#![feature(impl_trait_in_assoc_type)]
#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::result_large_err)]
#![recursion_limit = "256"]

extern crate core;

// Define macros at crate root so they're available throughout the crate
/// Create a client error with optional response code
#[macro_export]
macro_rules! mq_client_err {
    // Handle errors with a custom ResponseCode and formatted string
    ($response_code:expr, $fmt:expr, $($arg:expr),*) => {{
        let formatted_msg = format!($fmt, $($arg),*);
        let error_message = format!("CODE: {}  DESC: {}", $response_code as i32, formatted_msg);
        let faq_msg = rocketmq_common::common::FAQUrl::attach_default_url(Some(error_message.as_str()));
        rocketmq_error::RocketMQError::illegal_argument(faq_msg)
    }};

    ($response_code:expr, $error_message:expr) => {{
        let error_message = format!("CODE: {}  DESC: {}", $response_code as i32, $error_message);
        let faq_msg = rocketmq_common::common::FAQUrl::attach_default_url(Some(error_message.as_str()));
        rocketmq_error::RocketMQError::illegal_argument(faq_msg)
    }};

    // Handle errors without a ResponseCode, using only the error message (accepts both &str and String)
    ($error_message:expr) => {{
        let error_msg: &str = "Body is empty";
        let faq_msg = rocketmq_common::common::FAQUrl::attach_default_url(Some(error_msg));
        rocketmq_error::RocketMQError::illegal_argument(faq_msg)
    }};
}

/// Create a broker operation error
#[macro_export]
macro_rules! client_broker_err {
    // Handle errors with a custom ResponseCode and formatted string
    ($response_code:expr, $error_message:expr, $broker_addr:expr) => {{
        rocketmq_error::RocketMQError::broker_operation_failed(
            "BROKER_OPERATION",
            $response_code as i32,
            $error_message,
        )
        .with_broker_addr($broker_addr)
    }};
    // Handle errors without a ResponseCode, using only the error message
    ($response_code:expr, $error_message:expr) => {{
        rocketmq_error::RocketMQError::broker_operation_failed(
            "BROKER_OPERATION",
            $response_code as i32,
            $error_message,
        )
    }};
}

// Define client_error module
pub mod client_error;

pub mod admin;
pub mod base;
pub mod common;
pub mod consumer;
pub mod factory;
mod hook;
pub mod implementation;
mod latency;
pub mod producer;
mod trace;
pub mod utils;

pub use crate::consumer::consumer_impl::pull_request_ext::PullResultExt;
