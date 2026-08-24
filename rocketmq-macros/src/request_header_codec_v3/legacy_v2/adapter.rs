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

use std::collections::HashSet;

use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Fields, LitStr};

use super::super::canonical::{option_inner, resolve_protocol_path};
use super::super::model::{
    classify_type, AliasConflict, CodecProfile, FieldModel, HeaderModel, LegacyShim, LookupPlan, MissingPolicy,
    WireName,
};
use crate::request_header_codec_v2::attr::{parse_container_attrs, parse_field_attrs};
use crate::snake_to_camel_case;

pub(super) fn adapt(input: DeriveInput) -> syn::Result<HeaderModel> {
    let container = parse_container_attrs(&input.attrs)?;
    let protocol_path = match container.protocol_path {
        Some(path) => path,
        None => resolve_protocol_path(
            input.ident.span(),
            "#[request_header_codec_v2(crate = \"path::to::protocol\")]",
        )?,
    };
    let named = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            Fields::Unnamed(fields) => {
                return Err(syn::Error::new(
                    fields.span(),
                    "RequestHeaderCodecV2 requires a named struct",
                ));
            }
            Fields::Unit => {
                return Err(syn::Error::new(
                    input.ident.span(),
                    "RequestHeaderCodecV2 does not support unit structs",
                ));
            }
        },
        Data::Enum(data) => {
            return Err(syn::Error::new(
                data.enum_token.span(),
                "RequestHeaderCodecV2 does not support enums",
            ));
        }
        Data::Union(data) => {
            return Err(syn::Error::new(
                data.union_token.span(),
                "RequestHeaderCodecV2 does not support unions",
            ));
        }
    };

    let generic_types: HashSet<String> = input
        .generics
        .type_params()
        .map(|parameter| parameter.ident.to_string())
        .collect();
    let mut fields = Vec::with_capacity(named.len());
    let mut errors = None;
    for (index, field) in named.into_iter().enumerate() {
        match adapt_field(field, &generic_types, index) {
            Ok(field) => fields.push(field),
            Err(error) => super::super::combine_error(&mut errors, error),
        }
    }
    if let Some(error) = errors {
        return Err(error);
    }

    let ident_span = input.ident.span();
    Ok(HeaderModel {
        ident: input.ident,
        generics: input.generics,
        type_id: LitStr::new("legacy::RequestHeaderCodecV2", ident_span),
        java_class: None,
        validate_path: None,
        lookup: LookupPlan::Auto,
        legacy_shim: LegacyShim::Manual,
        protocol_path,
        fast: false,
        fields,
        profile: CodecProfile::LegacyV2 {
            validation_method: container.validation_method,
        },
    })
}

fn adapt_field(field: syn::Field, generic_types: &HashSet<String>, source_order: usize) -> syn::Result<FieldModel> {
    let span = field.span();
    let ident = field
        .ident
        .clone()
        .ok_or_else(|| syn::Error::new(span, "RequestHeaderCodecV2 requires named fields"))?;
    let attrs = parse_field_attrs(&field.attrs)?;
    let option_inner = option_inner(&field.ty).cloned();
    let base_type = option_inner.as_ref().unwrap_or(&field.ty);
    let flattened = attrs.flatten;
    let key = attrs.rename.as_ref().map_or_else(
        || WireName {
            value: snake_to_camel_case(&ident.to_string()),
            span: ident.span(),
        },
        |rename| WireName {
            value: rename.value(),
            span: rename.span(),
        },
    );
    let aliases = attrs
        .aliases
        .iter()
        .map(|alias| WireName {
            value: alias.value(),
            span: alias.span(),
        })
        .collect();
    let missing = if flattened {
        None
    } else if let Some(path) = &attrs.default_path {
        Some(MissingPolicy::DefaultWith(path.clone()))
    } else if attrs.has_default {
        Some(MissingPolicy::Default)
    } else if option_inner.is_some() {
        Some(MissingPolicy::Optional)
    } else if attrs.required {
        Some(MissingPolicy::Required)
    } else {
        Some(MissingPolicy::Default)
    };
    let kind = classify_type(base_type, generic_types, flattened);

    let model = FieldModel {
        ident,
        ty: field.ty,
        option_inner,
        key,
        aliases,
        alias_conflict: AliasConflict::PreferCanonical,
        missing,
        default_semantic: None,
        flattened,
        flatten_presence: None,
        java_type: None,
        range: None,
        binary_order: None,
        source_order: u16::try_from(source_order).unwrap_or(u16::MAX),
        kind,
        legacy_required: attrs.required,
        legacy_v2_default_declared: attrs.has_default,
        span,
    };
    super::validate::validate_field_shape(&model)?;
    Ok(model)
}
