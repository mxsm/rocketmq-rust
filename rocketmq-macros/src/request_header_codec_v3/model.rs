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

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Fields, GenericArgument, Generics, Ident, LitStr, Path, PathArguments, Type};

use super::attr::{parse_container_attrs, parse_field_attrs, FieldAttrs};
use super::combine_error;
use crate::snake_to_camel_case;

pub(super) struct HeaderModel {
    pub(super) ident: Ident,
    pub(super) generics: Generics,
    pub(super) type_id: LitStr,
    pub(super) java_class: Option<LitStr>,
    pub(super) validate_path: Option<Path>,
    pub(super) lookup: LookupPlan,
    pub(super) protocol_path: Path,
    pub(super) fast: bool,
    pub(super) fields: Vec<FieldModel>,
}

pub(super) struct FieldModel {
    pub(super) ident: Ident,
    pub(super) ty: Type,
    pub(super) option_inner: Option<Type>,
    pub(super) key: WireName,
    pub(super) aliases: Vec<WireName>,
    pub(super) alias_conflict: AliasConflict,
    pub(super) missing: Option<MissingPolicy>,
    pub(super) default_semantic: Option<SpannedString>,
    pub(super) flattened: bool,
    pub(super) flatten_presence: Option<FlattenPresence>,
    pub(super) java_type: Option<SpannedString>,
    pub(super) range: Option<HeaderRange>,
    pub(super) binary_order: Option<(u16, Span)>,
    pub(super) source_order: u16,
    pub(super) kind: ValueKind,
    pub(super) legacy_required: bool,
    pub(super) span: Span,
}

#[derive(Clone)]
pub(super) struct WireName {
    pub(super) value: String,
    pub(super) span: Span,
}

#[derive(Clone)]
pub(super) struct SpannedString {
    pub(super) value: String,
    pub(super) span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LookupPlan {
    Auto,
    Scan,
    Get,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AliasConflict {
    Error,
    PreferCanonical,
}

#[derive(Clone)]
pub(super) enum MissingPolicy {
    Optional,
    Required,
    Default,
    DefaultWith(Path),
}

impl PartialEq for MissingPolicy {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (Self::Optional, Self::Optional)
                | (Self::Required, Self::Required)
                | (Self::Default, Self::Default)
                | (Self::DefaultWith(_), Self::DefaultWith(_))
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FlattenPresence {
    Always,
    Any,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum HeaderRange {
    I32,
    I64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ValueKind {
    String,
    Bool,
    I32,
    I64,
    U32,
    U64,
    BoundaryType,
    Generic,
    Nested,
    Unsupported,
}

impl HeaderModel {
    #[cfg(test)]
    pub(super) fn parse(input: DeriveInput) -> syn::Result<Self> {
        let span = input.ident.span();
        let (model, errors) = Self::parse_partial(input);
        if let Some(error) = errors {
            return Err(error);
        }
        model.ok_or_else(|| syn::Error::new(span, "RequestHeaderCodecV3 could not construct a header model"))
    }

    pub(super) fn parse_partial(input: DeriveInput) -> (Option<Self>, Option<syn::Error>) {
        let mut errors = None;
        let (attrs, attribute_errors) = parse_container_attrs(&input.attrs);
        if let Some(error) = attribute_errors {
            combine_error(&mut errors, error);
        }

        let type_id = attrs.type_id.unwrap_or_else(|| {
            combine_error(
                &mut errors,
                syn::Error::new(
                    input.ident.span(),
                    "RequestHeaderCodecV3 requires header(type_id = \"...\")",
                ),
            );
            LitStr::new("invalid::MissingTypeId", input.ident.span())
        });
        let lookup = parse_lookup(attrs.lookup, &mut errors);
        let protocol_path = match attrs.protocol_path {
            Some(path) => path,
            None => match resolve_protocol_path(input.ident.span()) {
                Ok(path) => path,
                Err(error) => {
                    combine_error(&mut errors, error);
                    syn::parse_quote!(::rocketmq_protocol)
                }
            },
        };

        let generic_types: HashSet<String> = input
            .generics
            .type_params()
            .map(|parameter| parameter.ident.to_string())
            .collect();
        let named = match input.data {
            Data::Struct(data) => match data.fields {
                Fields::Named(fields) => Some(fields.named),
                Fields::Unnamed(fields) => {
                    combine_error(
                        &mut errors,
                        syn::Error::new(fields.span(), "RequestHeaderCodecV3 requires a named struct"),
                    );
                    None
                }
                Fields::Unit => {
                    combine_error(
                        &mut errors,
                        syn::Error::new(input.ident.span(), "RequestHeaderCodecV3 does not support unit structs"),
                    );
                    None
                }
            },
            Data::Enum(data) => {
                combine_error(
                    &mut errors,
                    syn::Error::new(data.enum_token.span(), "RequestHeaderCodecV3 does not support enums"),
                );
                None
            }
            Data::Union(data) => {
                combine_error(
                    &mut errors,
                    syn::Error::new(data.union_token.span(), "RequestHeaderCodecV3 does not support unions"),
                );
                None
            }
        };

        let valid_shape = named.is_some();
        let mut fields = Vec::with_capacity(named.as_ref().map_or(0, syn::punctuated::Punctuated::len));
        if let Some(named) = named {
            for (source_order, field) in named.into_iter().enumerate() {
                let Ok(source_order) = u16::try_from(source_order) else {
                    combine_error(
                        &mut errors,
                        syn::Error::new(field.span(), "RequestHeaderCodecV3 supports at most 65536 fields"),
                    );
                    continue;
                };
                match FieldModel::parse(field, &generic_types, source_order) {
                    Ok(field) => fields.push(field),
                    Err(error) => combine_error(&mut errors, error),
                }
            }
        }

        let model = valid_shape.then_some(Self {
            ident: input.ident,
            generics: input.generics,
            type_id,
            java_class: attrs.java_class,
            validate_path: attrs.validate,
            lookup,
            protocol_path,
            fast: attrs.fast.is_some(),
            fields,
        });
        (model, errors)
    }
}

impl FieldModel {
    fn parse(field: syn::Field, generic_types: &HashSet<String>, source_order: u16) -> syn::Result<Self> {
        let span = field.span();
        let ident = field
            .ident
            .clone()
            .ok_or_else(|| syn::Error::new(span, "RequestHeaderCodecV3 requires named fields"))?;
        let (attrs, mut errors) = parse_field_attrs(&field.attrs);
        let option_inner = option_inner(&field.ty).cloned();
        let base_type = option_inner.as_ref().unwrap_or(&field.ty);
        let flattened = attrs.flatten.is_some();
        let kind = classify_type(base_type, generic_types, flattened);
        let key = attrs.key.as_ref().map_or_else(
            || WireName {
                value: snake_to_camel_case(&ident.to_string()),
                span: ident.span(),
            },
            |key| WireName {
                value: key.value(),
                span: key.span(),
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

        let alias_conflict = parse_alias_conflict(attrs.alias_conflict.as_ref(), &mut errors);
        let flatten_presence = parse_flatten_presence(attrs.presence.as_ref(), &mut errors);
        let range = parse_range(attrs.range.as_ref(), &mut errors);
        let missing = normalize_missing(&attrs, option_inner.is_some(), flattened, span, &mut errors);
        let default_semantic = attrs.default_semantic.as_ref().map(|semantic| SpannedString {
            value: semantic.value(),
            span: semantic.span(),
        });
        let java_type = attrs.java_type.as_ref().map(|java_type| SpannedString {
            value: java_type.value(),
            span: java_type.span(),
        });

        validate_field_attribute_shape(
            &attrs,
            flattened,
            option_inner.is_some(),
            kind,
            flatten_presence,
            attrs.presence.is_some(),
            default_semantic.as_ref(),
            span,
            &mut errors,
        );

        if let Some(error) = errors {
            return Err(error);
        }

        Ok(Self {
            ident,
            ty: field.ty,
            option_inner,
            key,
            aliases,
            alias_conflict,
            missing,
            default_semantic,
            flattened,
            flatten_presence,
            java_type,
            range,
            binary_order: attrs.binary_order,
            source_order,
            kind,
            legacy_required: attrs.legacy_required.is_some(),
            span,
        })
    }

    pub(super) fn stable_order(&self) -> u16 {
        self.binary_order.map_or(self.source_order, |(order, _)| order)
    }
}

fn parse_lookup(value: Option<LitStr>, errors: &mut Option<syn::Error>) -> LookupPlan {
    let Some(value) = value else {
        return LookupPlan::Auto;
    };
    match value.value().as_str() {
        "auto" => LookupPlan::Auto,
        "scan" => LookupPlan::Scan,
        "get" => LookupPlan::Get,
        _ => {
            combine_error(
                errors,
                syn::Error::new(value.span(), "lookup must be one of: auto, scan, get"),
            );
            LookupPlan::Auto
        }
    }
}

fn parse_alias_conflict(value: Option<&LitStr>, errors: &mut Option<syn::Error>) -> AliasConflict {
    let Some(value) = value else {
        return AliasConflict::Error;
    };
    match value.value().as_str() {
        "error" => AliasConflict::Error,
        "prefer_canonical" => AliasConflict::PreferCanonical,
        _ => {
            combine_error(
                errors,
                syn::Error::new(value.span(), "alias_conflict must be error or prefer_canonical"),
            );
            AliasConflict::Error
        }
    }
}

fn parse_flatten_presence(value: Option<&LitStr>, errors: &mut Option<syn::Error>) -> Option<FlattenPresence> {
    let value = value?;
    match value.value().as_str() {
        "always" => Some(FlattenPresence::Always),
        "any" => Some(FlattenPresence::Any),
        _ => {
            combine_error(errors, syn::Error::new(value.span(), "presence must be always or any"));
            None
        }
    }
}

fn parse_range(value: Option<&LitStr>, errors: &mut Option<syn::Error>) -> Option<HeaderRange> {
    let value = value?;
    match value.value().as_str() {
        "i32" => Some(HeaderRange::I32),
        "i64" => Some(HeaderRange::I64),
        _ => {
            combine_error(errors, syn::Error::new(value.span(), "range must be i32 or i64"));
            None
        }
    }
}

fn normalize_missing(
    attrs: &FieldAttrs,
    optional: bool,
    flattened: bool,
    span: Span,
    errors: &mut Option<syn::Error>,
) -> Option<MissingPolicy> {
    if flattened {
        return None;
    }

    let required = attrs.required.is_some() || attrs.legacy_required.is_some();
    let count = usize::from(required)
        + usize::from(attrs.default.is_some())
        + usize::from(attrs.default_with_declared.is_some());
    if count > 1 {
        combine_error(
            errors,
            syn::Error::new(span, "required, default, and default_with are mutually exclusive"),
        );
    }
    if required && optional {
        combine_error(errors, syn::Error::new(span, "required cannot be used on Option<T>"));
    }

    if required {
        Some(MissingPolicy::Required)
    } else if attrs.default.is_some() {
        Some(MissingPolicy::Default)
    } else if let Some(path) = &attrs.default_with {
        Some(MissingPolicy::DefaultWith(path.clone()))
    } else if attrs.default_with_declared.is_some() {
        None
    } else if optional {
        Some(MissingPolicy::Optional)
    } else {
        combine_error(
            errors,
            syn::Error::new(span, "non-Option fields require required, default, or default_with"),
        );
        None
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "attribute groups are kept explicit for diagnostic spans"
)]
fn validate_field_attribute_shape(
    attrs: &FieldAttrs,
    flattened: bool,
    optional: bool,
    kind: ValueKind,
    presence: Option<FlattenPresence>,
    presence_declared: bool,
    default_semantic: Option<&SpannedString>,
    span: Span,
    errors: &mut Option<syn::Error>,
) {
    let has_default = attrs.default.is_some() || attrs.default_with_declared.is_some();
    if has_default && default_semantic.is_none() {
        combine_error(
            errors,
            syn::Error::new(span, "default and default_with require default_semantic"),
        );
    }
    if !has_default {
        if let Some(semantic) = default_semantic {
            combine_error(
                errors,
                syn::Error::new(semantic.span, "default_semantic requires default or default_with"),
            );
        }
    }
    if let Some(semantic) = default_semantic {
        let valid = semantic.value.starts_with("literal:")
            || semantic
                .value
                .strip_prefix("dynamic:")
                .is_some_and(|identifier| !identifier.is_empty());
        if !valid {
            combine_error(
                errors,
                syn::Error::new(
                    semantic.span,
                    "default_semantic must be literal:<wire-text> or dynamic:<semantic-id>",
                ),
            );
        }
    }

    if flattened {
        if attrs.key.is_some()
            || !attrs.aliases.is_empty()
            || attrs.required.is_some()
            || attrs.legacy_required.is_some()
            || attrs.default.is_some()
            || attrs.default_with_declared.is_some()
            || attrs.default_semantic.is_some()
            || attrs.alias_conflict.is_some()
            || attrs.java_type.is_some()
            || attrs.range.is_some()
        {
            combine_error(
                errors,
                syn::Error::new(
                    span,
                    "flatten cannot be combined with key, alias, missing policy, default, alias_conflict, java_type, or range",
                ),
            );
        }
        if optional && !presence_declared {
            combine_error(
                errors,
                syn::Error::new(span, "Option<Flattened> requires presence = \"any\" or \"always\""),
            );
        }
        if !optional && presence == Some(FlattenPresence::Any) {
            combine_error(
                errors,
                syn::Error::new(span, "presence = \"any\" is valid only for Option<Flattened>"),
            );
        }
        if !matches!(kind, ValueKind::Nested | ValueKind::Generic) {
            combine_error(errors, syn::Error::new(span, "flatten requires a nested header type"));
        }
    } else if attrs.presence.is_some() {
        combine_error(
            errors,
            syn::Error::new(span, "presence is valid only for flatten fields"),
        );
    }
}

fn option_inner(ty: &Type) -> Option<&Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let segment = type_path.path.segments.last()?;
    if segment.ident != "Option" {
        return None;
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    if arguments.args.len() != 1 {
        return None;
    }
    match arguments.args.first()? {
        GenericArgument::Type(inner) => Some(inner),
        _ => None,
    }
}

fn classify_type(ty: &Type, generic_types: &HashSet<String>, flattened: bool) -> ValueKind {
    let Type::Path(type_path) = ty else {
        return ValueKind::Unsupported;
    };
    if type_path.qself.is_some() {
        return ValueKind::Unsupported;
    }
    let Some(segment) = type_path.path.segments.last() else {
        return ValueKind::Unsupported;
    };
    if generic_types.contains(&segment.ident.to_string()) && matches!(segment.arguments, PathArguments::None) {
        return ValueKind::Generic;
    }
    if !matches!(segment.arguments, PathArguments::None) {
        return if flattened {
            ValueKind::Nested
        } else {
            ValueKind::Unsupported
        };
    }
    match segment.ident.to_string().as_str() {
        "String" | "CheetahString" => ValueKind::String,
        "bool" => ValueKind::Bool,
        "i32" => ValueKind::I32,
        "i64" => ValueKind::I64,
        "u32" => ValueKind::U32,
        "u64" => ValueKind::U64,
        "BoundaryType" => ValueKind::BoundaryType,
        _ if flattened => ValueKind::Nested,
        _ => ValueKind::Unsupported,
    }
}

fn resolve_protocol_path(span: Span) -> syn::Result<Path> {
    use proc_macro_crate::{crate_name, FoundCrate};

    match crate_name("rocketmq-protocol") {
        Ok(FoundCrate::Itself) => Ok(syn::parse_quote!(crate)),
        Ok(FoundCrate::Name(name)) => {
            let ident = Ident::new(&name.replace('-', "_"), span);
            Ok(syn::parse_quote!(::#ident))
        }
        Err(error) => Err(syn::Error::new(
            span,
            format!("unable to resolve rocketmq-protocol ({error}); use #[header(crate = \"path::to::protocol\")]"),
        )),
    }
}
