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
#![allow(dead_code)]
#![feature(sync_unsafe_cell)]
#![feature(duration_constructors)]
#![feature(fn_traits)]
extern crate core;

pub mod clients;
pub mod code;
pub mod codec;
pub mod connection;
pub mod net;
pub mod protocol;

pub use crate::protocol::rocketmq_serializable;

pub mod base;
pub mod common;
pub mod remoting;
pub mod remoting_server;
pub mod request_processor;
pub mod rpc;
pub mod runtime;

// Error helpers for unified error system
pub mod error_helpers;
pub mod smart_encode_buffer;
