// Copyright 2026 The RocketMQ Rust Authors
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

use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use super::Filter;
use super::FilterCompileError;
use super::SqlFilter;
use crate::expression::EvaluationContext;
use crate::expression::EvaluationError;
use crate::expression::Expression;
use crate::expression::Value;

/// Process-local identity retained by registry snapshots and their cache keys.
#[derive(Clone)]
pub struct FilterRegistryId(Arc<()>);

impl PartialEq for FilterRegistryId {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for FilterRegistryId {}

impl Hash for FilterRegistryId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state);
    }
}

impl fmt::Debug for FilterRegistryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FilterRegistryId")
    }
}

/// A registry configuration failure that contains no submitted filter text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterRegistryError {
    EmptyType,
    DuplicateType,
    MissingSql92,
}

impl fmt::Display for FilterRegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::EmptyType => "filter type must not be empty",
            Self::DuplicateType => "filter type is already registered",
            Self::MissingSql92 => "filter registry requires SQL92",
        })
    }
}

impl std::error::Error for FilterRegistryError {}

/// Builds one immutable registry. Building consumes the only mutation handle.
#[derive(Debug, Default)]
pub struct FilterRegistryBuilder {
    filters: HashMap<String, Arc<dyn Filter>>,
}

impl FilterRegistryBuilder {
    /// Creates a builder containing the built-in SQL92 compiler.
    pub fn with_sql92() -> Self {
        Self {
            filters: HashMap::from([("SQL92".to_owned(), Arc::new(SqlFilter::new()) as Arc<dyn Filter>)]),
        }
    }

    /// Registers a compiler without replacing an existing implementation.
    ///
    /// # Errors
    /// Returns an error for an empty or duplicate type name.
    pub fn register(&mut self, filter: Arc<dyn Filter>) -> Result<(), FilterRegistryError> {
        let name = filter.of_type();
        if name.is_empty() {
            return Err(FilterRegistryError::EmptyType);
        }
        match self.filters.entry(name.to_owned()) {
            std::collections::hash_map::Entry::Occupied(_) => Err(FilterRegistryError::DuplicateType),
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(filter);
                Ok(())
            }
        }
    }

    /// Freezes the registry for the lifetime of a service instance.
    ///
    /// # Errors
    /// Returns an error if the required SQL92 compiler is absent.
    pub fn build(self) -> Result<Arc<FilterRegistrySnapshot>, FilterRegistryError> {
        if !self.filters.contains_key("SQL92") {
            return Err(FilterRegistryError::MissingSql92);
        }
        Ok(self.freeze())
    }

    fn freeze(self) -> Arc<FilterRegistrySnapshot> {
        Arc::new(FilterRegistrySnapshot {
            id: FilterRegistryId(Arc::new(())),
            filters: self.filters,
        })
    }
}

/// Immutable compiler bindings for one service instance.
#[derive(Debug)]
pub struct FilterRegistrySnapshot {
    id: FilterRegistryId,
    filters: HashMap<String, Arc<dyn Filter>>,
}

impl FilterRegistrySnapshot {
    /// Creates an isolated registry with the built-in SQL92 implementation.
    pub fn sql92() -> Arc<Self> {
        FilterRegistryBuilder::with_sql92().freeze()
    }

    pub fn id(&self) -> FilterRegistryId {
        self.id.clone()
    }

    /// Returns the compiler bound at construction time.
    pub fn get(&self, expression_type: &str) -> Option<Arc<dyn Filter>> {
        self.filters.get(expression_type).cloned()
    }

    /// Compiles an expression while retaining its registry through execution.
    ///
    /// # Errors
    /// Returns a typed unsupported-type or compiler failure.
    pub fn compile(self: &Arc<Self>, expression_type: &str, text: &str) -> Result<CompiledFilter, FilterBindingError> {
        let compiler = self.get(expression_type).ok_or(FilterBindingError::UnsupportedType)?;
        let expression = compiler.try_compile(text).map_err(FilterBindingError::Compile)?;
        Ok(CompiledFilter {
            registry: Arc::clone(self),
            expression,
        })
    }
}

/// Safe failure data suitable for an instance-owned negative cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterBindingError {
    UnsupportedType,
    Compile(FilterCompileError),
}

impl fmt::Display for FilterBindingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedType => f.write_str("unsupported filter type"),
            Self::Compile(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for FilterBindingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::UnsupportedType => None,
            Self::Compile(error) => Some(error),
        }
    }
}

/// Runtime-only expression retaining the registry that supplied its compiler.
pub struct CompiledFilter {
    registry: Arc<FilterRegistrySnapshot>,
    expression: Box<dyn Expression>,
}

impl CompiledFilter {
    pub fn registry_id(&self) -> FilterRegistryId {
        self.registry.id()
    }
}

impl fmt::Display for CompiledFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.expression.fmt(f)
    }
}

impl Expression for CompiledFilter {
    fn evaluate(&self, context: &dyn EvaluationContext) -> Result<Value, EvaluationError> {
        self.expression.evaluate(context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::AlwaysFalseExpression;
    use crate::expression::AlwaysTrueExpression;
    use crate::expression::EmptyEvaluationContext;

    #[derive(Debug)]
    struct ConstantCompiler(bool);

    impl Filter for ConstantCompiler {
        fn of_type(&self) -> &str {
            "SQL92"
        }

        fn try_compile(&self, _: &str) -> Result<Box<dyn Expression>, FilterCompileError> {
            if self.0 {
                Ok(Box::new(AlwaysTrueExpression))
            } else {
                Ok(Box::new(AlwaysFalseExpression))
            }
        }
    }

    #[test]
    fn frozen_registry_preserves_compiler_identity_through_execution() {
        let make = |value| {
            let mut builder = FilterRegistryBuilder::default();
            builder.register(Arc::new(ConstantCompiler(value))).unwrap();
            builder.build().unwrap()
        };
        let first = make(true);
        let second = make(false);
        assert_ne!(first.id(), second.id());
        let first_weak = Arc::downgrade(&first);
        let compiled = first.compile("SQL92", "same expression").unwrap();
        assert_eq!(compiled.registry_id(), first.id());
        drop(first);
        assert!(first_weak.upgrade().is_some());
        assert_eq!(compiled.evaluate(&EmptyEvaluationContext), Ok(Value::Boolean(true)));
        assert_eq!(
            second
                .compile("SQL92", "same expression")
                .unwrap()
                .evaluate(&EmptyEvaluationContext),
            Ok(Value::Boolean(false))
        );
        drop(compiled);
        assert!(first_weak.upgrade().is_none());
    }

    #[test]
    fn builder_rejects_missing_required_and_duplicate_compilers() {
        assert_eq!(
            FilterRegistryBuilder::default().build().unwrap_err(),
            FilterRegistryError::MissingSql92
        );
        let mut builder = FilterRegistryBuilder::with_sql92();
        assert_eq!(
            builder.register(Arc::new(ConstantCompiler(false))),
            Err(FilterRegistryError::DuplicateType)
        );
        let registry = builder.build().unwrap();
        assert_eq!(
            registry
                .compile("SQL92", "1 = 1")
                .unwrap()
                .evaluate(&EmptyEvaluationContext),
            Ok(Value::Boolean(true))
        );
        // No mutation operation is exposed by FilterRegistrySnapshot.
        assert!(matches!(
            registry.compile("missing", "input"),
            Err(FilterBindingError::UnsupportedType)
        ));
    }
}
