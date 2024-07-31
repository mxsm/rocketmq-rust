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
use std::any::Any;

use rocketmq_common::ArcRefCellWrapper;

use crate::error::Error;
use crate::protocol::command_custom_header::CommandCustomHeader;

#[derive(Default)]
pub struct RpcResponse {
    pub code: i32,
    pub header: Option<ArcRefCellWrapper<Box<dyn CommandCustomHeader + Send + Sync + 'static>>>,
    pub body: Option<Box<dyn Any>>,
    pub exception: Option<Error>,
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

    pub fn new_exception(exception: Option<Error>) -> Self {
        Self {
            code: exception.as_ref().map_or(0, |e| match e {
                Error::RpcException(code, _) => *code,
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
            header: Some(ArcRefCellWrapper::new(header)),
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
