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

use syn::spanned::Spanned;
use syn::{Attribute, Expr, Ident, LitStr, Path, Token};

#[derive(Default)]
pub(crate) struct ContainerAttrs {
    pub(crate) validation_method: Option<Ident>,
    pub(crate) protocol_path: Option<Path>,
}

#[derive(Default)]
pub(crate) struct FieldAttrs {
    pub(crate) required: bool,
    pub(crate) flatten: bool,
    pub(crate) rename: Option<LitStr>,
    pub(crate) aliases: Vec<LitStr>,
    pub(crate) default_path: Option<Path>,
    pub(crate) has_default: bool,
}

pub(crate) fn parse_container_attrs(attrs: &[Attribute]) -> syn::Result<ContainerAttrs> {
    let mut parsed = ContainerAttrs::default();
    for attr in attrs {
        if !attr.path().is_ident("request_header") && !attr.path().is_ident("request_header_codec_v2") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("validate") {
                let value: LitStr = meta.value()?.parse()?;
                let ident = value
                    .parse::<Ident>()
                    .map_err(|_| syn::Error::new(value.span(), "validate must name a method on the header type"))?;
                set_once(
                    &mut parsed.validation_method,
                    ident,
                    meta.path.span(),
                    "duplicate validate option",
                )?;
                return Ok(());
            }
            if meta.path.is_ident("crate") {
                let value: LitStr = meta.value()?.parse()?;
                let path = value
                    .parse::<Path>()
                    .map_err(|error| syn::Error::new(value.span(), format!("invalid protocol crate path: {error}")))?;
                set_once(
                    &mut parsed.protocol_path,
                    path,
                    meta.path.span(),
                    "duplicate crate option",
                )?;
                return Ok(());
            }
            Err(meta.error("unsupported RequestHeaderCodecV2 container option"))
        })?;
    }
    Ok(parsed)
}

pub(crate) fn parse_field_attrs(attrs: &[Attribute]) -> syn::Result<FieldAttrs> {
    let mut parsed = FieldAttrs::default();
    for attr in attrs {
        if attr.path().is_ident("required") {
            if parsed.required {
                return Err(syn::Error::new(attr.span(), "duplicate required attribute"));
            }
            parsed.required = true;
            continue;
        }
        if !attr.path().is_ident("serde") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("flatten") {
                parsed.flatten = true;
                return Ok(());
            }
            if meta.path.is_ident("rename") {
                let value: LitStr = meta.value()?.parse()?;
                set_once(&mut parsed.rename, value, meta.path.span(), "duplicate serde rename")?;
                return Ok(());
            }
            if meta.path.is_ident("alias") {
                parsed.aliases.push(meta.value()?.parse()?);
                return Ok(());
            }
            if meta.path.is_ident("default") {
                if parsed.has_default {
                    return Err(meta.error("duplicate serde default"));
                }
                parsed.has_default = true;
                if meta.input.peek(Token![=]) {
                    let value: LitStr = meta.value()?.parse()?;
                    parsed.default_path = Some(value.parse::<Path>().map_err(|error| {
                        syn::Error::new(value.span(), format!("invalid serde default path: {error}"))
                    })?);
                }
                return Ok(());
            }

            // Serde attributes unrelated to request-header mapping remain Serde's concern.
            // Consume their value so combined attributes such as rename + skip_serializing_if
            // can still be parsed without weakening validation of the options above.
            if meta.input.peek(Token![=]) {
                let _: Expr = meta.value()?.parse()?;
            }
            Ok(())
        })?;
    }
    Ok(parsed)
}

fn set_once<T>(slot: &mut Option<T>, value: T, span: proc_macro2::Span, message: &str) -> syn::Result<()> {
    if slot.is_some() {
        return Err(syn::Error::new(span, message));
    }
    *slot = Some(value);
    Ok(())
}
