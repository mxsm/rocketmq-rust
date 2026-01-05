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

//! Message Filter Module
//!
//! This module provides a pluggable filter architecture for RocketMQ message filtering.
//! It includes the core `Filter` trait (SPI), a factory for filter management, and
//! default implementations like SQL-92 filtering.
//!
//! # Architecture
//!
//! The filter system consists of three main components:
//!
//! 1. **Filter Trait (SPI)**: Defines the interface for all filter implementations
//! 2. **Filter Factory**: Manages filter registration and retrieval
//! 3. **Filter Implementations**: Concrete implementations like `SqlFilter`
//!
//! # Usage
//!
//! ## Getting a Filter
//!
//! ```rust,ignore
//! use rocketmq_filter::filter::FilterFactory;
//!
//! // Get the default SQL-92 filter
//! let sql_filter = FilterFactory::get_sql_filter();
//!
//! // Or get by type
//! let filter = FilterFactory::instance().get("SQL92").unwrap();
//! ```
//!
//! ## Compiling Expressions
//!
//! ```rust,ignore
//! use rocketmq_filter::filter::{Filter, FilterFactory};
//!
//! let filter = FilterFactory::get_sql_filter();
//! let expr = filter.compile("age > 18 AND region = 'US'")?;
//! ```
//!
//! ## Registering Custom Filters
//!
//! ```rust,ignore
//! use rocketmq_filter::filter::{Filter, FilterFactory};
//! use std::sync::Arc;
//!
//! // Implement the Filter trait
//! struct CustomFilter;
//! impl Filter for CustomFilter {
//!     fn compile(&self, expr: &str) -> Result<Box<dyn Expression>, FilterError> {
//!         // Custom implementation
//!     }
//!     
//!     fn of_type(&self) -> &str {
//!         "CUSTOM"
//!     }
//! }
//!
//! // Register it
//! let factory = FilterFactory::instance();
//! factory.register(Arc::new(CustomFilter));
//! ```
//!
//! # Thread Safety
//!
//! All components in this module are designed for concurrent use:
//! - Filters implement `Send + Sync`
//! - The factory uses `DashMap` for lock-free concurrent access
//! - Filters are typically wrapped in `Arc` for shared ownership

mod filter_factory;
mod filter_spi;
mod filter_sql_filter;

pub use filter_factory::FilterFactory;
pub use filter_spi::Filter;
pub use filter_spi::FilterError;
pub use filter_spi::FilterSpi;
pub use filter_sql_filter::SqlFilter;
