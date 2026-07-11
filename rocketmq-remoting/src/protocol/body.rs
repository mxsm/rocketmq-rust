// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Canonical body schemas plus owner-side compatibility facades.

pub use rocketmq_protocol::protocol::body::*;

pub mod subscription_group_wrapper;
pub mod topic_info_wrapper;
