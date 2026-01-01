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

#![allow(dead_code)]
#![allow(clippy::result_large_err)]

pub use self::kvconfig::kvconfig_mananger::KVConfigManager;
pub use self::namesrv_config_parse::parse_command_and_config_file;
pub use self::route::route_info_manager::RouteInfoManager;
pub use self::route::route_info_manager_v2::RouteInfoManagerV2;
pub use self::route::route_info_manager_wrapper::RouteInfoManagerWrapper;

pub mod bootstrap;
mod kvconfig;
mod namesrv_config_parse;
pub mod processor;
pub mod route;
mod route_info;
