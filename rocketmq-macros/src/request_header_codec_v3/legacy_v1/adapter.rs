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

use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Fields, LitStr};

use super::super::model::{
    AliasConflict, CodecProfile, FieldModel, HeaderModel, LegacyShim, LookupPlan, MissingPolicy, ValueKind, WireName,
};
use crate::{has_serde_flatten_attribute, is_option_type, is_struct_type, snake_to_camel_case};

pub(super) fn adapt(input: DeriveInput) -> Option<HeaderModel> {
    let fields = match input.data {
        Data::Struct(value) => match value.fields {
            Fields::Named(fields) => fields.named,
            Fields::Unnamed(_) | Fields::Unit => return None,
        },
        Data::Enum(_) | Data::Union(_) => return None,
    };

    let fields = fields
        .into_iter()
        .enumerate()
        .filter_map(|(source_order, field)| adapt_field(field, source_order))
        .collect();

    Some(HeaderModel {
        ident: input.ident,
        generics: input.generics,
        type_id: LitStr::new("legacy::V1", proc_macro2::Span::call_site()),
        java_class: None,
        validate_path: None,
        lookup: LookupPlan::Auto,
        legacy_shim: LegacyShim::Manual,
        protocol_path: syn::parse_quote!(crate),
        fast: false,
        fields,
        profile: CodecProfile::LegacyV1,
    })
}

fn adapt_field(field: syn::Field, source_order: usize) -> Option<FieldModel> {
    let span = field.span();
    let ident = field.ident.clone()?;
    let legacy_struct_type = is_struct_type(&field.ty);
    let legacy_serde_flatten = has_serde_flatten_attribute(&field);
    let required = field
        .attrs
        .iter()
        .any(|attr| attr.path().get_ident().is_some_and(|ident| ident == "required"));
    let option_inner = is_option_type(&field.ty).cloned();
    let flattened = legacy_struct_type && legacy_serde_flatten;
    let missing = if flattened {
        None
    } else if required {
        Some(MissingPolicy::Required)
    } else if option_inner.is_some() {
        Some(MissingPolicy::Optional)
    } else {
        Some(MissingPolicy::Default)
    };

    Some(FieldModel {
        key: WireName {
            value: snake_to_camel_case(&format!("{ident}")),
            span: ident.span(),
        },
        ident,
        ty: field.ty,
        option_inner,
        aliases: Vec::new(),
        alias_conflict: AliasConflict::PreferCanonical,
        missing,
        default_semantic: None,
        flattened,
        flatten_presence: None,
        java_type: None,
        range: None,
        binary_order: None,
        source_order: u16::try_from(source_order).unwrap_or(u16::MAX),
        kind: ValueKind::Unsupported,
        legacy_required: required,
        legacy_v2_default_declared: false,
        span,
    })
}
