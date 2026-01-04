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

//! Filter Factory for Managing Filter Implementations
//!
//! This module provides a centralized factory for registering and retrieving
//! filter implementations. It supports dynamic filter registration and lookup
//! by type identifier.
//!
//! # Architecture
//!
//! The factory uses a global registry pattern with interior mutability to allow
//! runtime filter registration while maintaining thread safety through `Arc` and
//! `DashMap`.
//!
//! # Examples
//!
//! ```rust,ignore
//! use rocketmq_filter::filter::{FilterFactory, SqlFilter};
//! use std::sync::Arc;
//!
//! // Get the SQL-92 filter
//! let sql_filter = FilterFactory::get_sql_filter();
//!
//! // Register a custom filter
//! let factory = FilterFactory::instance();
//! factory.register(Arc::new(CustomFilter::new()));
//!
//! // Retrieve by type
//! let filter = factory.get("SQL92");
//! ```

use std::sync::Arc;
use std::sync::LazyLock;

use dashmap::DashMap;

use crate::filter::filter_spi::Filter;
use crate::filter::filter_sql_filter::SqlFilter;

/// Global filter registry using DashMap for thread-safe concurrent access.
///
/// The registry maps filter type identifiers to filter implementations.
/// DashMap provides lock-free reads and fine-grained locking for writes.
static FILTER_REGISTRY: LazyLock<DashMap<String, Arc<dyn Filter>>> = LazyLock::new(|| {
    let registry = DashMap::new();
    // Register default filters
    registry.insert("SQL92".to_string(), Arc::new(SqlFilter::new()) as Arc<dyn Filter>);
    registry
});

/// Filter factory for managing and retrieving filter implementations.
///
/// `FilterFactory` provides a centralized registry for filter implementations,
/// supporting runtime registration, unregistration, and lookup by type.
///
/// # Singleton Pattern
///
/// The factory uses a singleton instance accessible via `FilterFactory::instance()`.
/// This ensures a single global registry for all filter implementations.
///
/// # Thread Safety
///
/// The factory is thread-safe and supports concurrent registration and lookup
/// operations through the use of `DashMap` for the internal registry.
///
/// # Examples
///
/// ```rust,ignore
/// use rocketmq_filter::filter::FilterFactory;
///
/// // Access the singleton instance
/// let factory = FilterFactory::instance();
///
/// // Get a registered filter
/// if let Some(filter) = factory.get("SQL92") {
///     let expr = filter.compile("age > 18")?;
/// }
/// ```
#[derive(Debug)]
pub struct FilterFactory;

impl FilterFactory {
    /// Returns the singleton filter factory instance.
    ///
    /// # Returns
    ///
    /// A reference to the global `FilterFactory` instance.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let factory = FilterFactory::instance();
    /// let filter = factory.get("SQL92");
    /// ```
    pub fn instance() -> &'static Self {
        static INSTANCE: LazyLock<FilterFactory> = LazyLock::new(|| FilterFactory);
        &INSTANCE
    }

    /// Registers a new filter implementation.
    ///
    /// The filter is registered under its type identifier (obtained via `filter.of_type()`).
    /// If a filter with the same type already exists, it will be replaced.
    ///
    /// # Arguments
    ///
    /// * `filter` - The filter implementation to register
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use rocketmq_filter::filter::{FilterFactory, SqlFilter};
    /// use std::sync::Arc;
    ///
    /// let factory = FilterFactory::instance();
    /// let custom_filter = Arc::new(SqlFilter::new());
    /// factory.register(custom_filter);
    /// ```
    ///
    /// # Thread Safety
    ///
    /// This method is thread-safe and can be called concurrently from multiple threads.
    pub fn register(&self, filter: Arc<dyn Filter>) {
        let filter_type = filter.of_type().to_string();
        FILTER_REGISTRY.insert(filter_type, filter);
    }

    /// Unregisters a filter by its type identifier.
    ///
    /// Removes the filter with the specified type from the registry.
    /// Returns the removed filter if it existed, or `None` if no filter
    /// with that type was registered.
    ///
    /// # Arguments
    ///
    /// * `filter_type` - The type identifier of the filter to remove
    ///
    /// # Returns
    ///
    /// * `Some(Arc<dyn Filter>)` - The removed filter
    /// * `None` - No filter with the specified type was registered
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let factory = FilterFactory::instance();
    /// if let Some(removed) = factory.unregister("CUSTOM") {
    ///     println!("Removed filter: {:?}", removed);
    /// }
    /// ```
    ///
    /// # Thread Safety
    ///
    /// This method is thread-safe and can be called concurrently from multiple threads.
    pub fn unregister(&self, filter_type: &str) -> Option<Arc<dyn Filter>> {
        FILTER_REGISTRY.remove(filter_type).map(|(_, v)| v)
    }

    /// Retrieves a registered filter by its type identifier.
    ///
    /// Returns a cloned `Arc` to the filter implementation if one is registered
    /// under the specified type, or `None` if no such filter exists.
    ///
    /// # Arguments
    ///
    /// * `filter_type` - The type identifier of the filter to retrieve
    ///
    /// # Returns
    ///
    /// * `Some(Arc<dyn Filter>)` - The filter implementation
    /// * `None` - No filter with the specified type is registered
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let factory = FilterFactory::instance();
    /// if let Some(filter) = factory.get("SQL92") {
    ///     let expr = filter.compile("age > 18")?;
    /// }
    /// ```
    ///
    /// # Performance
    ///
    /// This operation is lock-free for reads and performs an atomic reference
    /// count increment on the returned `Arc`.
    pub fn get(&self, filter_type: &str) -> Option<Arc<dyn Filter>> {
        FILTER_REGISTRY.get(filter_type).map(|entry| Arc::clone(&*entry))
    }

    /// Retrieves the default SQL-92 filter implementation.
    ///
    /// This is a convenience method that returns the pre-registered SQL-92 filter.
    /// It is equivalent to calling `factory.get("SQL92")` but provides a more
    /// ergonomic API for the common case of SQL filtering.
    ///
    /// # Returns
    ///
    /// An `Arc` to the SQL-92 filter implementation.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use rocketmq_filter::filter::FilterFactory;
    ///
    /// let sql_filter = FilterFactory::get_sql_filter();
    /// let expr = sql_filter.compile("price > 100")?;
    /// ```
    ///
    /// # Panics
    ///
    /// This method will panic if the SQL-92 filter is not registered, which
    /// should never happen under normal circumstances as it's registered
    /// during static initialization.
    pub fn get_sql_filter() -> Arc<dyn Filter> {
        FILTER_REGISTRY
            .get("SQL92")
            .map(|entry| Arc::clone(&*entry))
            .expect("SQL92 filter should be registered by default")
    }

    /// Returns a list of all registered filter type identifiers.
    ///
    /// This method is useful for debugging and introspection to see which
    /// filters are currently available in the registry.
    ///
    /// # Returns
    ///
    /// A vector of filter type identifiers.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let factory = FilterFactory::instance();
    /// let types = factory.registered_types();
    /// for filter_type in types {
    ///     println!("Registered filter: {}", filter_type);
    /// }
    /// ```
    pub fn registered_types(&self) -> Vec<String> {
        FILTER_REGISTRY.iter().map(|entry| entry.key().clone()).collect()
    }

    /// Returns the number of registered filters.
    ///
    /// # Returns
    ///
    /// The count of currently registered filter implementations.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let factory = FilterFactory::instance();
    /// println!("Number of filters: {}", factory.count());
    /// ```
    pub fn count(&self) -> usize {
        FILTER_REGISTRY.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_sql_filter() {
        let filter = FilterFactory::get_sql_filter();
        assert_eq!(filter.of_type(), "SQL92");
    }

    #[test]
    fn test_factory_instance() {
        let factory1 = FilterFactory::instance();
        let factory2 = FilterFactory::instance();
        // Both should be the same instance
        assert!(std::ptr::eq(factory1, factory2));
    }

    #[test]
    fn test_get_registered_filter() {
        let factory = FilterFactory::instance();
        let filter = factory.get("SQL92");
        assert!(filter.is_some());
        assert_eq!(filter.unwrap().of_type(), "SQL92");
    }

    #[test]
    fn test_get_nonexistent_filter() {
        let factory = FilterFactory::instance();
        let filter = factory.get("NONEXISTENT");
        assert!(filter.is_none());
    }

    #[test]
    fn test_register_new_filter() {
        let factory = FilterFactory::instance();
        let new_filter = Arc::new(SqlFilter::new());

        // Register under a custom name
        factory.register(new_filter);

        // Should be able to retrieve it
        let retrieved = factory.get("SQL92");
        assert!(retrieved.is_some());
    }

    #[test]
    fn test_registered_types() {
        let factory = FilterFactory::instance();
        let types = factory.registered_types();
        assert!(types.contains(&"SQL92".to_string()));
    }

    #[test]
    fn test_count() {
        let factory = FilterFactory::instance();
        let count = factory.count();
        assert!(count >= 1); // At least SQL92 should be registered
    }
}
