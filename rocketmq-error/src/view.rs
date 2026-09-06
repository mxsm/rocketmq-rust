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

use std::fmt;
use std::iter::FusedIterator;
use std::slice;

use crate::context::ErrorContextField;
use crate::context::FieldValueRef;
use crate::CanonicalCondition;
use crate::ContextVisibility;
use crate::ErrorCode;
use crate::ErrorContext;
use crate::ErrorDescriptor;
use crate::ErrorSeverity;
use crate::Exposure;
use crate::FieldSchema;
use crate::ProjectionSpec;
use crate::RecoveryHint;

/// A descriptor/context contract violation discovered while creating a safe view.
///
/// Violations carry only catalog schema metadata. They never include a runtime
/// context value, source error, or backtrace.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ViewContextViolation {
    /// A retained context field is not declared by the selected descriptor.
    UndeclaredField {
        /// The selected descriptor's stable code.
        descriptor: ErrorCode,
        /// The schema retained by the context.
        actual: FieldSchema,
    },
    /// A retained context field has the same name as a descriptor field but a different schema.
    SchemaMismatch {
        /// The selected descriptor's stable code.
        descriptor: ErrorCode,
        /// The schema required by the descriptor.
        expected: FieldSchema,
        /// The schema retained by the context.
        actual: FieldSchema,
    },
}

impl fmt::Debug for ViewContextViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UndeclaredField { descriptor, actual } => formatter
                .debug_struct("UndeclaredField")
                .field("descriptor", descriptor)
                .field("actual", actual)
                .finish(),
            Self::SchemaMismatch {
                descriptor,
                expected,
                actual,
            } => formatter
                .debug_struct("SchemaMismatch")
                .field("descriptor", descriptor)
                .field("expected", expected)
                .field("actual", actual)
                .finish(),
        }
    }
}

impl fmt::Display for ViewContextViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UndeclaredField { descriptor, actual } => {
                write!(
                    formatter,
                    "context field `{}` is not declared by {descriptor}",
                    actual.name()
                )
            }
            Self::SchemaMismatch {
                descriptor,
                expected,
                actual,
            } => write!(
                formatter,
                "context field `{}` does not match the schema declared by {descriptor}: expected {:?}, got {:?}",
                actual.name(),
                expected,
                actual,
            ),
        }
    }
}

/// A borrowed public projection of one descriptor-validated error context.
///
/// This view exposes only the descriptor's stable identity, fixed public
/// message, and descriptor-owned protocol projections. Descriptors with
/// [`Exposure::Public`] may additionally expose declared public context fields;
/// [`Exposure::Generic`] descriptors expose no dynamic fields. The view borrows
/// its inputs and performs no allocation after the [`ErrorContext`] has been
/// constructed.
///
/// ```compile_fail,E0616
/// use rocketmq_error::{ErrorContext, PublicErrorView, ROUTE_TOPIC_NOT_FOUND};
///
/// let context = ErrorContext::new();
/// let view = PublicErrorView::try_new(&ROUTE_TOPIC_NOT_FOUND, &context).unwrap();
/// let _ = view.context;
/// ```
pub struct PublicErrorView<'a> {
    descriptor: &'static ErrorDescriptor,
    context: &'a ErrorContext,
}

static EMPTY_ERROR_CONTEXT: ErrorContext = ErrorContext::new();

impl PublicErrorView<'static> {
    /// Creates a public view containing descriptor-owned data only.
    ///
    /// This is the safe fallback when retained context fails descriptor
    /// validation. It preserves the original descriptor and performs no
    /// allocation.
    #[inline]
    pub const fn descriptor_only(descriptor: &'static ErrorDescriptor) -> Self {
        Self {
            descriptor,
            context: &EMPTY_ERROR_CONTEXT,
        }
    }
}

impl<'a> PublicErrorView<'a> {
    /// Validates `context` against `descriptor` and creates a public safe view.
    ///
    /// Every retained context field must exactly match a schema declared by
    /// `descriptor`. Missing fields are valid because descriptor schemas do
    /// not currently define required values.
    ///
    /// # Errors
    ///
    /// Returns [`ViewContextViolation`] for the first retained context field
    /// that is undeclared or has a different complete schema.
    pub fn try_new(
        descriptor: &'static ErrorDescriptor,
        context: &'a ErrorContext,
    ) -> Result<Self, ViewContextViolation> {
        validate_context(descriptor, context)?;
        Ok(Self { descriptor, context })
    }

    /// Returns the stable catalog code.
    #[inline]
    pub const fn code(&self) -> ErrorCode {
        self.descriptor.code()
    }

    /// Returns the descriptor's fixed public message.
    #[inline]
    pub const fn message(&self) -> &'static str {
        self.descriptor.public_message()
    }

    /// Iterates permitted public fields in descriptor declaration order.
    ///
    /// Generic-exposure descriptors always return an empty iterator.
    #[inline]
    pub fn fields(&self) -> PublicFields<'a> {
        PublicFields::new(self.descriptor, self.context)
    }

    /// Returns descriptor-owned boundary projections.
    #[inline]
    pub const fn projection(&self) -> ProjectionSpec {
        self.descriptor.projection()
    }

    /// Returns whether context construction normalized, discarded, or truncated a value.
    #[inline]
    pub const fn is_truncated(&self) -> bool {
        self.context.is_truncated()
    }
}

impl fmt::Debug for PublicErrorView<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PublicErrorView")
            .field("code", &self.code())
            .field("message", &self.message())
            .field("fields", &self.fields())
            .field("projection", &self.projection())
            .field("truncated", &self.is_truncated())
            .finish()
    }
}

/// A borrowed controlled-diagnostics projection of one descriptor-validated context.
///
/// `DiagnosticView` exposes descriptor-approved diagnostic fields and
/// key-only secret-presence markers for operational diagnostics. It never
/// exposes source errors, caller locations, or backtraces and must not be
/// serialized into public protocol responses. Like [`PublicErrorView`], it
/// borrows its inputs and performs no allocation after [`ErrorContext`] has
/// been constructed.
pub struct DiagnosticView<'a> {
    descriptor: &'static ErrorDescriptor,
    context: &'a ErrorContext,
}

impl<'a> DiagnosticView<'a> {
    /// Validates `context` against `descriptor` and creates a controlled diagnostic view.
    ///
    /// # Errors
    ///
    /// Returns [`ViewContextViolation`] for the first retained context field
    /// that is undeclared or has a different complete schema.
    pub fn try_new(
        descriptor: &'static ErrorDescriptor,
        context: &'a ErrorContext,
    ) -> Result<Self, ViewContextViolation> {
        validate_context(descriptor, context)?;
        Ok(Self { descriptor, context })
    }

    /// Returns the stable catalog code.
    #[inline]
    pub const fn code(&self) -> ErrorCode {
        self.descriptor.code()
    }

    /// Returns the descriptor's fixed public message.
    #[inline]
    pub const fn message(&self) -> &'static str {
        self.descriptor.public_message()
    }

    /// Returns the descriptor's protocol-independent canonical condition.
    #[inline]
    pub const fn condition(&self) -> CanonicalCondition {
        self.descriptor.condition()
    }

    /// Returns the descriptor's operational severity.
    #[inline]
    pub const fn severity(&self) -> ErrorSeverity {
        self.descriptor.severity()
    }

    /// Returns the descriptor's recovery advice.
    #[inline]
    pub const fn recovery_hint(&self) -> RecoveryHint {
        self.descriptor.recovery_hint()
    }

    /// Iterates present public, diagnostic, and redacted secret-presence fields.
    #[inline]
    pub fn fields(&self) -> DiagnosticFields<'a> {
        DiagnosticFields::new(self.descriptor, self.context)
    }

    /// Returns descriptor-owned boundary projections.
    #[inline]
    pub const fn projection(&self) -> ProjectionSpec {
        self.descriptor.projection()
    }

    /// Returns whether context construction normalized, discarded, or truncated a value.
    #[inline]
    pub const fn is_truncated(&self) -> bool {
        self.context.is_truncated()
    }
}

impl fmt::Debug for DiagnosticView<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DiagnosticView")
            .field("code", &self.code())
            .field("message", &self.message())
            .field("condition", &self.condition())
            .field("severity", &self.severity())
            .field("recovery_hint", &self.recovery_hint())
            .field("fields", &self.fields())
            .field("projection", &self.projection())
            .field("truncated", &self.is_truncated())
            .finish()
    }
}

/// A borrowed descriptor-ordered iterator over public context fields.
#[derive(Clone)]
pub struct PublicFields<'a> {
    inner: ViewFields<'a>,
    enabled: bool,
}

impl<'a> PublicFields<'a> {
    fn new(descriptor: &'static ErrorDescriptor, context: &'a ErrorContext) -> Self {
        Self {
            inner: ViewFields::new(descriptor, context),
            enabled: matches!(descriptor.exposure(), Exposure::Public),
        }
    }
}

impl<'a> Iterator for PublicFields<'a> {
    type Item = ViewFieldRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.enabled {
            self.inner.next_field(false)
        } else {
            None
        }
    }
}

impl FusedIterator for PublicFields<'_> {}

impl fmt::Debug for PublicFields<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_list().entries(self.clone()).finish()
    }
}

/// A borrowed descriptor-ordered iterator over controlled diagnostic fields.
#[derive(Clone)]
pub struct DiagnosticFields<'a> {
    inner: ViewFields<'a>,
}

impl<'a> DiagnosticFields<'a> {
    fn new(descriptor: &'static ErrorDescriptor, context: &'a ErrorContext) -> Self {
        Self {
            inner: ViewFields::new(descriptor, context),
        }
    }
}

impl<'a> Iterator for DiagnosticFields<'a> {
    type Item = ViewFieldRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next_field(true)
    }
}

impl FusedIterator for DiagnosticFields<'_> {}

impl fmt::Debug for DiagnosticFields<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_list().entries(self.clone()).finish()
    }
}

#[derive(Clone)]
struct ViewFields<'a> {
    descriptor_fields: slice::Iter<'static, FieldSchema>,
    context: &'a ErrorContext,
}

impl<'a> ViewFields<'a> {
    fn new(descriptor: &'static ErrorDescriptor, context: &'a ErrorContext) -> Self {
        Self {
            descriptor_fields: descriptor.fields().iter(),
            context,
        }
    }

    fn next_field(&mut self, include_diagnostic: bool) -> Option<ViewFieldRef<'a>> {
        for schema in self.descriptor_fields.by_ref() {
            if !include_visibility(schema.visibility(), include_diagnostic) {
                continue;
            }
            let Some(field) = self.context.fields().iter().find(|field| field.schema() == *schema) else {
                continue;
            };
            return Some(ViewFieldRef { field });
        }
        None
    }
}

fn include_visibility(visibility: ContextVisibility, include_diagnostic: bool) -> bool {
    match visibility {
        ContextVisibility::Public => true,
        ContextVisibility::Diagnostic | ContextVisibility::SecretPresenceOnly => include_diagnostic,
    }
}

/// A borrowed field exposed by a descriptor-validated view.
pub struct ViewFieldRef<'a> {
    field: &'a ErrorContextField,
}

impl<'a> ViewFieldRef<'a> {
    /// Returns the descriptor-approved external field name.
    #[inline]
    pub const fn name(&self) -> &'static str {
        self.field.name()
    }

    /// Returns the descriptor-approved field visibility.
    #[inline]
    pub const fn visibility(&self) -> ContextVisibility {
        self.field.visibility()
    }

    /// Returns the safe borrowed value for this field.
    ///
    /// Secret-presence fields always return [`ViewValueRef::Redacted`].
    #[inline]
    pub fn value(&self) -> ViewValueRef<'a> {
        match self.field.value() {
            FieldValueRef::Text(value) => ViewValueRef::Text(value),
            FieldValueRef::I64(value) => ViewValueRef::I64(value),
            FieldValueRef::U64(value) => ViewValueRef::U64(value),
            FieldValueRef::Bool(value) => ViewValueRef::Bool(value),
            FieldValueRef::Presence => ViewValueRef::Redacted,
        }
    }
}

impl fmt::Debug for ViewFieldRef<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ViewFieldRef")
            .field("name", &self.name())
            .field("visibility", &self.visibility())
            .field("value", &self.value())
            .finish()
    }
}

/// A safe borrowed value exposed by a descriptor-validated view.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ViewValueRef<'a> {
    /// Bounded normalized text.
    Text(&'a str),
    /// A signed integer.
    I64(i64),
    /// An unsigned integer.
    U64(u64),
    /// A Boolean value.
    Bool(bool),
    /// A key-only marker for secret-bearing input.
    Redacted,
}

impl fmt::Debug for ViewValueRef<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text(value) => value.fmt(formatter),
            Self::I64(value) => value.fmt(formatter),
            Self::U64(value) => value.fmt(formatter),
            Self::Bool(value) => value.fmt(formatter),
            Self::Redacted => formatter.write_str("<redacted>"),
        }
    }
}

fn validate_context(descriptor: &'static ErrorDescriptor, context: &ErrorContext) -> Result<(), ViewContextViolation> {
    for field in context.fields() {
        let actual = field.schema();
        let Some(expected) = descriptor
            .fields()
            .iter()
            .find(|expected| expected.name() == actual.name())
        else {
            return Err(ViewContextViolation::UndeclaredField {
                descriptor: descriptor.code(),
                actual,
            });
        };
        if *expected != actual {
            return Err(ViewContextViolation::SchemaMismatch {
                descriptor: descriptor.code(),
                expected: *expected,
                actual,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fields;

    const BOOLEAN_CODE: ErrorCode = match ErrorCode::try_new("test.view.boolean") {
        Some(code) => code,
        None => panic!("synthetic test code must be valid"),
    };
    static BOOLEAN_FIELDS: [FieldSchema; 1] = [fields::ATTEMPTED.schema()];
    static BOOLEAN_DESCRIPTOR: ErrorDescriptor = match ErrorDescriptor::try_new(
        BOOLEAN_CODE,
        crate::ErrorClass::INTERNAL,
        CanonicalCondition::Internal,
        crate::FaultAttribution::Unknown,
        crate::ComponentId::CORE,
        "Synthetic Boolean view test",
        ErrorSeverity::Info,
        RecoveryHint::Never,
        crate::BacktracePolicy::Never,
        crate::Exposure::Public,
        crate::ROUTE_TOPIC_NOT_FOUND.projection(),
        &BOOLEAN_FIELDS,
    ) {
        Some(descriptor) => descriptor,
        None => panic!("synthetic Boolean descriptor must be valid"),
    };

    const GENERIC_CODE: ErrorCode = match ErrorCode::try_new("test.view.generic") {
        Some(code) => code,
        None => panic!("synthetic test code must be valid"),
    };
    static GENERIC_FIELDS: [FieldSchema; 1] = [fields::ACTUAL_BYTES.schema()];
    static GENERIC_DESCRIPTOR: ErrorDescriptor = match ErrorDescriptor::try_new(
        GENERIC_CODE,
        crate::ErrorClass::INTERNAL,
        CanonicalCondition::Internal,
        crate::FaultAttribution::Unknown,
        crate::ComponentId::CORE,
        "Synthetic Generic view test",
        ErrorSeverity::Info,
        RecoveryHint::Never,
        crate::BacktracePolicy::Never,
        crate::Exposure::Generic,
        crate::ROUTE_TOPIC_NOT_FOUND.projection(),
        &GENERIC_FIELDS,
    ) {
        Some(descriptor) => descriptor,
        None => panic!("synthetic Generic descriptor must be valid"),
    };

    #[test]
    fn synthetic_boolean_descriptor_exposes_a_borrowed_boolean_value() {
        let context = ErrorContext::new().with_bool(fields::ATTEMPTED, true);
        let public = PublicErrorView::try_new(&BOOLEAN_DESCRIPTOR, &context).expect("public view");
        let diagnostic = DiagnosticView::try_new(&BOOLEAN_DESCRIPTOR, &context).expect("diagnostic view");

        assert!(public.fields().next().is_none());
        assert_eq!(
            diagnostic.fields().next().expect("Boolean field").value(),
            ViewValueRef::Bool(true)
        );
    }

    #[test]
    fn generic_exposure_suppresses_descriptor_declared_public_fields() {
        let context = ErrorContext::new().with_u64(fields::ACTUAL_BYTES, 42);
        let public = PublicErrorView::try_new(&GENERIC_DESCRIPTOR, &context).expect("public view");
        let diagnostic = DiagnosticView::try_new(&GENERIC_DESCRIPTOR, &context).expect("diagnostic view");

        assert!(public.fields().next().is_none());
        assert_eq!(
            diagnostic.fields().next().expect("diagnostic field").value(),
            ViewValueRef::U64(42)
        );
    }

    #[test]
    fn descriptor_only_view_preserves_descriptor_policy_without_context() {
        let view = PublicErrorView::descriptor_only(&crate::PROTOCOL_BODY_INVALID);

        assert_eq!(view.code(), crate::PROTOCOL_BODY_INVALID.code());
        assert_eq!(view.message(), crate::PROTOCOL_BODY_INVALID.public_message());
        assert_eq!(view.projection(), crate::PROTOCOL_BODY_INVALID.projection());
        assert!(view.fields().next().is_none());
        assert!(!view.is_truncated());
    }
}
