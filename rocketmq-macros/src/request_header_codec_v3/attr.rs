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

use proc_macro2::Span;
use syn::meta::ParseNestedMeta;
use syn::spanned::Spanned;
use syn::{Attribute, LitInt, LitStr, Meta, Path, Token};

use super::combine_error;

#[derive(Default)]
pub(super) struct ContainerAttrs {
    pub(super) type_id: Option<LitStr>,
    pub(super) java_class: Option<LitStr>,
    pub(super) validate: Option<Path>,
    pub(super) lookup: Option<LitStr>,
    pub(super) legacy_shim: Option<LitStr>,
    pub(super) protocol_path: Option<Path>,
    pub(super) fast: Option<Span>,
}

#[derive(Default)]
pub(super) struct FieldAttrs {
    pub(super) key: Option<LitStr>,
    pub(super) aliases: Vec<LitStr>,
    pub(super) required: Option<Span>,
    pub(super) legacy_required: Option<Span>,
    pub(super) default: Option<Span>,
    pub(super) default_with_declared: Option<Span>,
    pub(super) default_with: Option<Path>,
    pub(super) default_semantic: Option<LitStr>,
    pub(super) alias_conflict: Option<LitStr>,
    pub(super) flatten: Option<Span>,
    pub(super) presence: Option<LitStr>,
    pub(super) java_type: Option<LitStr>,
    pub(super) range: Option<LitStr>,
    pub(super) binary_order: Option<(u16, Span)>,
}

pub(super) fn parse_container_attrs(attrs: &[Attribute]) -> (ContainerAttrs, Option<syn::Error>) {
    let mut parsed = ContainerAttrs::default();
    let mut errors = None;

    for attr in attrs.iter().filter(|attr| attr.path().is_ident("header")) {
        let result = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("type_id") {
                let value = parse_lit_str(&meta)?;
                set_once(
                    &mut parsed.type_id,
                    value,
                    meta.path.span(),
                    "duplicate type_id",
                    &mut errors,
                );
                return Ok(());
            }
            if meta.path.is_ident("java_class") {
                let value = parse_lit_str(&meta)?;
                set_once(
                    &mut parsed.java_class,
                    value,
                    meta.path.span(),
                    "duplicate java_class",
                    &mut errors,
                );
                return Ok(());
            }
            if meta.path.is_ident("validate") {
                if let Some(value) = parse_path(&meta, "validate", &mut errors)? {
                    set_once(
                        &mut parsed.validate,
                        value,
                        meta.path.span(),
                        "duplicate validate",
                        &mut errors,
                    );
                }
                return Ok(());
            }
            if meta.path.is_ident("lookup") {
                let value = parse_lit_str(&meta)?;
                set_once(
                    &mut parsed.lookup,
                    value,
                    meta.path.span(),
                    "duplicate lookup",
                    &mut errors,
                );
                return Ok(());
            }
            if meta.path.is_ident("legacy_shim") {
                let value = parse_lit_str(&meta)?;
                set_once(
                    &mut parsed.legacy_shim,
                    value,
                    meta.path.span(),
                    "duplicate legacy_shim",
                    &mut errors,
                );
                return Ok(());
            }
            if meta.path.is_ident("crate") {
                if let Some(value) = parse_path(&meta, "protocol crate", &mut errors)? {
                    set_once(
                        &mut parsed.protocol_path,
                        value,
                        meta.path.span(),
                        "duplicate crate option",
                        &mut errors,
                    );
                }
                return Ok(());
            }
            if meta.path.is_ident("fast") {
                set_once(
                    &mut parsed.fast,
                    meta.path.span(),
                    meta.path.span(),
                    "duplicate fast",
                    &mut errors,
                );
                parse_flag(&meta, "fast")?;
                return Ok(());
            }
            Err(meta.error("unsupported RequestHeaderCodecV3 container option"))
        });
        if let Err(error) = result {
            combine_error(&mut errors, error);
        }
    }

    (parsed, errors)
}

pub(super) fn parse_field_attrs(attrs: &[Attribute]) -> (FieldAttrs, Option<syn::Error>) {
    let mut parsed = FieldAttrs::default();
    let mut errors = None;

    for attr in attrs {
        if attr.path().is_ident("required") {
            if !matches!(&attr.meta, Meta::Path(_)) {
                combine_error(
                    &mut errors,
                    syn::Error::new(attr.span(), "legacy required must be a flag without arguments"),
                );
            }
            set_once(
                &mut parsed.legacy_required,
                attr.span(),
                attr.span(),
                "duplicate legacy required attribute",
                &mut errors,
            );
            continue;
        }
        if !attr.path().is_ident("header") {
            continue;
        }

        let result = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("key") {
                let value = parse_lit_str(&meta)?;
                set_once(&mut parsed.key, value, meta.path.span(), "duplicate key", &mut errors);
                return Ok(());
            }
            if meta.path.is_ident("alias") {
                parsed.aliases.push(parse_lit_str(&meta)?);
                return Ok(());
            }
            if meta.path.is_ident("required") {
                set_once(
                    &mut parsed.required,
                    meta.path.span(),
                    meta.path.span(),
                    "duplicate required",
                    &mut errors,
                );
                parse_flag(&meta, "required")?;
                return Ok(());
            }
            if meta.path.is_ident("default") {
                set_once(
                    &mut parsed.default,
                    meta.path.span(),
                    meta.path.span(),
                    "duplicate default",
                    &mut errors,
                );
                parse_flag(&meta, "default")?;
                return Ok(());
            }
            if meta.path.is_ident("default_with") {
                set_once(
                    &mut parsed.default_with_declared,
                    meta.path.span(),
                    meta.path.span(),
                    "duplicate default_with",
                    &mut errors,
                );
                if let Some(value) = parse_path(&meta, "default_with", &mut errors)? {
                    if parsed.default_with.is_none() {
                        parsed.default_with = Some(value);
                    }
                }
                return Ok(());
            }
            if meta.path.is_ident("default_semantic") {
                let value = parse_lit_str(&meta)?;
                set_once(
                    &mut parsed.default_semantic,
                    value,
                    meta.path.span(),
                    "duplicate default_semantic",
                    &mut errors,
                );
                return Ok(());
            }
            if meta.path.is_ident("alias_conflict") {
                let value = parse_lit_str(&meta)?;
                set_once(
                    &mut parsed.alias_conflict,
                    value,
                    meta.path.span(),
                    "duplicate alias_conflict",
                    &mut errors,
                );
                return Ok(());
            }
            if meta.path.is_ident("flatten") {
                set_once(
                    &mut parsed.flatten,
                    meta.path.span(),
                    meta.path.span(),
                    "duplicate flatten",
                    &mut errors,
                );
                parse_flag(&meta, "flatten")?;
                return Ok(());
            }
            if meta.path.is_ident("presence") {
                let value = parse_lit_str(&meta)?;
                set_once(
                    &mut parsed.presence,
                    value,
                    meta.path.span(),
                    "duplicate presence",
                    &mut errors,
                );
                return Ok(());
            }
            if meta.path.is_ident("java_type") {
                let value = parse_lit_str(&meta)?;
                set_once(
                    &mut parsed.java_type,
                    value,
                    meta.path.span(),
                    "duplicate java_type",
                    &mut errors,
                );
                return Ok(());
            }
            if meta.path.is_ident("range") {
                let value = parse_lit_str(&meta)?;
                set_once(
                    &mut parsed.range,
                    value,
                    meta.path.span(),
                    "duplicate range",
                    &mut errors,
                );
                return Ok(());
            }
            if meta.path.is_ident("binary_order") {
                let value: LitInt = meta.value()?.parse()?;
                let order = value
                    .base10_parse::<u16>()
                    .map_err(|_| syn::Error::new(value.span(), "binary_order must fit in u16"))?;
                set_once(
                    &mut parsed.binary_order,
                    (order, value.span()),
                    meta.path.span(),
                    "duplicate binary_order",
                    &mut errors,
                );
                return Ok(());
            }
            Err(meta.error("unsupported RequestHeaderCodecV3 field option"))
        });
        if let Err(error) = result {
            combine_error(&mut errors, error);
        }
    }

    if let (Some(required), Some(_)) = (parsed.required, parsed.legacy_required) {
        combine_error(
            &mut errors,
            syn::Error::new(required, "required is declared by both #[header] and #[required]"),
        );
    }

    (parsed, errors)
}

fn parse_lit_str(meta: &ParseNestedMeta<'_>) -> syn::Result<LitStr> {
    meta.value()?.parse()
}

fn parse_path(meta: &ParseNestedMeta<'_>, name: &str, errors: &mut Option<syn::Error>) -> syn::Result<Option<Path>> {
    let value = parse_lit_str(meta)?;
    match value.parse::<Path>() {
        Ok(path) => Ok(Some(path)),
        Err(error) => {
            combine_error(
                errors,
                syn::Error::new(value.span(), format!("invalid {name} path: {error}")),
            );
            Ok(None)
        }
    }
}

fn parse_flag(meta: &ParseNestedMeta<'_>, name: &str) -> syn::Result<()> {
    if meta.input.peek(Token![=]) || meta.input.peek(syn::token::Paren) {
        Err(meta.error(format!("{name} is a flag and does not accept a value")))
    } else {
        Ok(())
    }
}

fn set_once<T>(slot: &mut Option<T>, value: T, span: Span, message: &str, errors: &mut Option<syn::Error>) {
    if slot.is_some() {
        combine_error(errors, syn::Error::new(span, message));
    } else {
        *slot = Some(value);
    }
}
