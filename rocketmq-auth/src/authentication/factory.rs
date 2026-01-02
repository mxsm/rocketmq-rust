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

//! Authentication Factory Module
//!
//! This module provides factory functions for creating authentication components.
//! It acts as the central point for instantiating authentication strategies, providers,
//! evaluators, and metadata managers.
//!
//! # Design Principles
//!
//! - **Factory Pattern**: Centralized creation logic for auth components
//! - **SPI-like Design**: Configurable strategies and providers via AuthConfig
//! - **Caching**: Singleton instances cached by config name for efficiency
//! - **Thread-safe**: All factory methods are safe for concurrent use

pub mod authentication_factory;

pub use authentication_factory::AuthenticationFactory;
