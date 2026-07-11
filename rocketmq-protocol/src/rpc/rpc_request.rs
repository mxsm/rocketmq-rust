// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::any::Any;

use crate::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use crate::protocol::CommandCustomHeader;

pub struct RpcRequest<H> {
    pub code: i32,
    pub header: H,
    pub body: Option<Box<dyn Any + Send>>,
}

impl<H> RpcRequest<H>
where
    H: CommandCustomHeader + TopicRequestHeaderTrait,
{
    pub fn new(code: i32, header: H, body: Option<Box<dyn Any + Send>>) -> Self {
        Self { code, header, body }
    }
}
