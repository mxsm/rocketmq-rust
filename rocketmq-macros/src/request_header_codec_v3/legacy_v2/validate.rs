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

use super::super::model::{FieldModel, HeaderModel};
use crate::{is_struct_type, snake_to_camel_case};

pub(crate) fn validate(model: &HeaderModel) -> syn::Result<()> {
    let mut errors = None;
    validate_wire_key_ownership(model, &mut errors);
    match errors {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

pub(super) fn validate_field_shape(field: &FieldModel) -> syn::Result<()> {
    let mut errors = None;
    collect_field_shape_errors(field, &mut errors);
    match errors {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn collect_field_shape_errors(field: &FieldModel, errors: &mut Option<syn::Error>) {
    if field.legacy_required && field.option_inner.is_some() {
        super::super::combine_error(
            errors,
            syn::Error::new(field.span, "required cannot be used on Option<T>"),
        );
    }
    if field.legacy_required && field.legacy_v2_default_declared {
        super::super::combine_error(
            errors,
            syn::Error::new(field.span, "required and serde(default) are mutually exclusive"),
        );
    }
    if field.flattened && !is_struct_type(&field.ty) {
        super::super::combine_error(
            errors,
            syn::Error::new(field.span, "serde(flatten) requires a nested header type"),
        );
    }
    if field.flattened
        && (!field.aliases.is_empty() || field.key.value != snake_to_camel_case(&field.ident.to_string()))
    {
        super::super::combine_error(
            errors,
            syn::Error::new(field.span, "serde(flatten) cannot be combined with rename or alias"),
        );
    }
    if field.key.value.is_empty() {
        super::super::combine_error(errors, syn::Error::new(field.span, "wire key must not be empty"));
    }
    for alias in &field.aliases {
        if alias.value.is_empty() {
            super::super::combine_error(errors, syn::Error::new(field.span, "wire alias must not be empty"));
        }
    }
}

fn validate_wire_key_ownership(model: &HeaderModel, errors: &mut Option<syn::Error>) {
    let mut owners: HashMap<&str, &syn::Ident> = HashMap::new();

    for field in model.fields.iter().filter(|field| !field.flattened) {
        for key in
            std::iter::once(field.key.value.as_str()).chain(field.aliases.iter().map(|alias| alias.value.as_str()))
        {
            if let Some(owner) = owners.insert(key, &field.ident) {
                super::super::combine_error(
                    errors,
                    syn::Error::new(
                        field.span,
                        format!("wire key `{key}` is already used by field `{owner}`"),
                    ),
                );
            }
        }
    }
}
