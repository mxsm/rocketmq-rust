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

use quote::ToTokens;
use syn::spanned::Spanned;
use syn::PathArguments;

use super::combine_error;
use super::model::CodecProfile;
use super::model::{
    AliasConflict, FieldModel, HeaderModel, HeaderRange, LookupPlan, MissingPolicy, ValueKind, WireName,
};

pub(super) fn validate(model: &HeaderModel) -> syn::Result<()> {
    match &model.profile {
        CodecProfile::V3 => {}
        CodecProfile::LegacyV2 { .. } => return super::legacy_v2::validate::validate(model),
        CodecProfile::LegacyV1 => return Ok(()),
    }

    let mut errors = None;
    validate_container(model, &mut errors);
    validate_fields(model, &mut errors);

    match errors {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn validate_container(model: &HeaderModel, errors: &mut Option<syn::Error>) {
    let type_id = model.type_id.value();
    match syn::parse_str::<syn::Path>(&type_id) {
        Ok(path)
            if path.leading_colon.is_none()
                && path.segments.len() >= 2
                && path
                    .segments
                    .iter()
                    .all(|segment| matches!(segment.arguments, PathArguments::None)) => {}
        _ => combine_error(
            errors,
            syn::Error::new(
                model.type_id.span(),
                "type_id must be a stable, fully qualified Rust path with at least two segments",
            ),
        ),
    }

    if let Some(java_class) = &model.java_class {
        let value = java_class.value();
        let valid = value.split('.').count() >= 2 && value.split('.').all(is_java_identifier);
        if !valid {
            combine_error(
                errors,
                syn::Error::new(
                    java_class.span(),
                    "java_class must be a fully qualified Java class name",
                ),
            );
        }
    }

    if model.protocol_path.segments.is_empty() {
        combine_error(
            errors,
            syn::Error::new(model.ident.span(), "protocol crate path must not be empty"),
        );
    }
    if model
        .validate_path
        .as_ref()
        .is_some_and(|path| path.segments.is_empty())
    {
        combine_error(
            errors,
            syn::Error::new(model.ident.span(), "validate path must not be empty"),
        );
    }

    // These values are code-generation decisions. Reading them here keeps the
    // validated model complete without adding an intermediate partial impl.
    let _ = model.generics.split_for_impl();
    let _ = model.fast;
    match model.lookup {
        LookupPlan::Auto | LookupPlan::Scan | LookupPlan::Get => {}
    }
}

fn validate_fields(model: &HeaderModel, errors: &mut Option<syn::Error>) {
    let mut key_owners: HashMap<&str, &syn::Ident> = HashMap::new();
    let mut order_owners: HashMap<u16, &syn::Ident> = HashMap::new();
    let has_java_contract = model.java_class.is_some();

    for field in &model.fields {
        validate_field(field, has_java_contract, errors);

        if !field.flattened {
            for name in std::iter::once(&field.key).chain(field.aliases.iter()) {
                validate_wire_name(name, errors);
                if let Some(owner) = key_owners.insert(name.value.as_str(), &field.ident) {
                    combine_error(
                        errors,
                        syn::Error::new(
                            name.span,
                            format!("wire key `{}` is already used by field `{owner}`", name.value),
                        ),
                    );
                }
            }
        }

        let order = field.stable_order();
        if let Some(owner) = order_owners.insert(order, &field.ident) {
            let span = field.binary_order.map_or(field.span, |(_, span)| span);
            combine_error(
                errors,
                syn::Error::new(
                    span,
                    format!("binary_order `{order}` is already used by field `{owner}`"),
                ),
            );
        }
    }
}

fn validate_field(field: &FieldModel, has_java_contract: bool, errors: &mut Option<syn::Error>) {
    if field.kind == ValueKind::Unsupported {
        combine_error(
            errors,
            syn::Error::new(
                field.ty.span(),
                format!(
                    "unsupported RequestHeaderCodecV3 field type `{}`",
                    field.ty.to_token_stream()
                ),
            ),
        );
    }

    if !field.flattened && field.aliases.is_empty() && field.alias_conflict != AliasConflict::Error {
        combine_error(
            errors,
            syn::Error::new(field.span, "alias_conflict requires at least one alias"),
        );
    }
    if field.aliases.len() > u16::MAX as usize {
        combine_error(
            errors,
            syn::Error::new(field.span, "a field supports at most 65535 decode aliases"),
        );
    }

    if let Some(MissingPolicy::DefaultWith(path)) = &field.missing {
        if path.segments.is_empty() {
            combine_error(
                errors,
                syn::Error::new(field.span, "default_with path must not be empty"),
            );
        }
    }
    let _ = field.option_inner.as_ref();
    let _ = field.default_semantic.as_ref();
    let _ = field.flatten_presence;
    validate_java_range(field, has_java_contract, errors);
}

fn validate_wire_name(name: &WireName, errors: &mut Option<syn::Error>) {
    if name.value.is_empty() {
        combine_error(
            errors,
            syn::Error::new(name.span, "wire key and alias must not be empty"),
        );
    }
    if name.value.bytes().any(|byte| byte == 0) {
        combine_error(
            errors,
            syn::Error::new(name.span, "wire key and alias must not contain NUL"),
        );
    }
    if name.value.len() > u16::MAX as usize {
        combine_error(
            errors,
            syn::Error::new(name.span, "wire key and alias must fit the ROCKETMQ u16 key length"),
        );
    }
}

fn validate_java_range(field: &FieldModel, has_java_contract: bool, errors: &mut Option<syn::Error>) {
    let java_type = field.java_type.as_ref().map(|value| value.value.as_str());
    let requires_java_range = has_java_contract || java_type.is_some();
    match (field.kind, field.range, java_type) {
        (ValueKind::U32, Some(HeaderRange::I32), None | Some("int" | "Integer")) => {}
        (ValueKind::U64, Some(HeaderRange::I64), None | Some("long" | "Long")) => {}
        (ValueKind::U32, Some(_), _) => combine_error(
            errors,
            syn::Error::new(
                field.span,
                "u32 fields require range = \"i32\"; java_type, when present, must be int or Integer",
            ),
        ),
        (ValueKind::U64, Some(_), _) => combine_error(
            errors,
            syn::Error::new(
                field.span,
                "u64 fields require range = \"i64\"; java_type, when present, must be long or Long",
            ),
        ),
        (ValueKind::U32, None, _) if requires_java_range => combine_error(
            errors,
            syn::Error::new(field.span, "unsigned u32 fields require range = \"i32\""),
        ),
        (ValueKind::U64, None, _) if requires_java_range => combine_error(
            errors,
            syn::Error::new(field.span, "unsigned u64 fields require range = \"i64\""),
        ),
        (ValueKind::I32, Some(_), _)
        | (ValueKind::I64, Some(_), _)
        | (ValueKind::Bool, Some(_), _)
        | (ValueKind::String, Some(_), _)
        | (ValueKind::BoundaryType, Some(_), _)
        | (ValueKind::Generic, Some(_), _)
        | (ValueKind::Nested, Some(_), _)
        | (ValueKind::Unsupported, Some(_), _) => combine_error(
            errors,
            syn::Error::new(
                field.span,
                "range is supported only for unsigned Rust fields mapped to signed Java integers",
            ),
        ),
        _ => {}
    }

    let valid_java_type = match field.kind {
        ValueKind::String => matches!(java_type, None | Some("String")),
        ValueKind::Bool => matches!(java_type, None | Some("boolean" | "Boolean")),
        ValueKind::I32 | ValueKind::U32 => matches!(java_type, None | Some("int" | "Integer")),
        ValueKind::I64 | ValueKind::U64 => matches!(java_type, None | Some("long" | "Long")),
        ValueKind::BoundaryType => matches!(java_type, None | Some("BoundaryType")),
        ValueKind::Generic | ValueKind::Nested | ValueKind::Unsupported => true,
    };
    if !valid_java_type {
        let span = field.java_type.as_ref().map_or(field.span, |value| value.span);
        combine_error(
            errors,
            syn::Error::new(span, "java_type is incompatible with the Rust header value type"),
        );
    }
}

fn is_java_identifier(component: &str) -> bool {
    let mut chars = component.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || matches!(first, '_' | '$'))
        && chars.all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '$'))
}
