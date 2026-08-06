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

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Fields, Generics, Ident, Path, Type};

use super::attr::{parse_container_attrs, parse_field_attrs};
use crate::{get_type_name, is_option_type, is_struct_type, snake_to_camel_case};

pub(super) struct HeaderModel {
    pub(super) ident: Ident,
    pub(super) generics: Generics,
    pub(super) fields: Vec<FieldModel>,
    pub(super) validation_method: Option<Ident>,
    pub(super) protocol_path: Path,
}

pub(super) struct FieldModel {
    pub(super) ident: Ident,
    pub(super) ty: Type,
    pub(super) inner_type: Option<Type>,
    pub(super) required: bool,
    pub(super) flattened: bool,
    pub(super) wire_key: String,
    pub(super) aliases: Vec<String>,
    pub(super) default_path: Option<Path>,
    pub(super) has_default: bool,
    pub(super) category: TypeCategory,
    pub(super) span: Span,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum TypeCategory {
    CheetahString,
    String,
    Primitive,
    Flattened,
}

impl HeaderModel {
    pub(super) fn parse(input: DeriveInput) -> syn::Result<Self> {
        let container = parse_container_attrs(&input.attrs)?;
        let protocol_path = match container.protocol_path {
            Some(path) => path,
            None => resolve_protocol_path(input.ident.span())?,
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

        let mut fields = Vec::with_capacity(named.len());
        let mut errors = None;
        for field in named {
            match FieldModel::parse(field) {
                Ok(field) => fields.push(field),
                Err(error) => combine_error(&mut errors, error),
            }
        }
        if let Some(error) = errors {
            return Err(error);
        }

        Ok(Self {
            ident: input.ident,
            generics: input.generics,
            fields,
            validation_method: container.validation_method,
            protocol_path,
        })
    }
}

impl FieldModel {
    fn parse(field: syn::Field) -> syn::Result<Self> {
        let span = field.span();
        let ident = field
            .ident
            .clone()
            .ok_or_else(|| syn::Error::new(span, "RequestHeaderCodecV2 requires named fields"))?;
        let attrs = parse_field_attrs(&field.attrs)?;
        let ty = field.ty;
        let inner_type = is_option_type(&ty).cloned();
        let base_type = inner_type.as_ref().unwrap_or(&ty);
        let type_name = get_type_name(base_type);
        let category = if attrs.flatten {
            TypeCategory::Flattened
        } else if type_name == "CheetahString" {
            TypeCategory::CheetahString
        } else if type_name == "String" {
            TypeCategory::String
        } else {
            TypeCategory::Primitive
        };
        let wire_key = attrs
            .rename
            .as_ref()
            .map_or_else(|| snake_to_camel_case(&ident.to_string()), syn::LitStr::value);
        let aliases = attrs.aliases.iter().map(syn::LitStr::value).collect();

        let model = Self {
            ident,
            ty,
            inner_type,
            required: attrs.required,
            flattened: attrs.flatten,
            wire_key,
            aliases,
            default_path: attrs.default_path,
            has_default: attrs.has_default,
            category,
            span,
        };
        model.validate_shape()?;
        Ok(model)
    }

    fn validate_shape(&self) -> syn::Result<()> {
        let mut errors = None;
        if self.required && self.inner_type.is_some() {
            combine_error(
                &mut errors,
                syn::Error::new(self.span, "required cannot be used on Option<T>"),
            );
        }
        if self.required && self.has_default {
            combine_error(
                &mut errors,
                syn::Error::new(self.span, "required and serde(default) are mutually exclusive"),
            );
        }
        if self.flattened && !is_struct_type(&self.ty) {
            combine_error(
                &mut errors,
                syn::Error::new(self.span, "serde(flatten) requires a nested header type"),
            );
        }
        if self.flattened && (!self.aliases.is_empty() || self.wire_key != snake_to_camel_case(&self.ident.to_string()))
        {
            combine_error(
                &mut errors,
                syn::Error::new(self.span, "serde(flatten) cannot be combined with rename or alias"),
            );
        }
        if self.wire_key.is_empty() {
            combine_error(&mut errors, syn::Error::new(self.span, "wire key must not be empty"));
        }
        for alias in &self.aliases {
            if alias.is_empty() {
                combine_error(&mut errors, syn::Error::new(self.span, "wire alias must not be empty"));
            }
        }
        match errors {
            Some(error) => Err(error),
            None => Ok(()),
        }
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
            format!(
                "unable to resolve rocketmq-protocol ({error}); use #[request_header_codec_v2(crate = \"path::to::protocol\")]"
            ),
        )),
    }
}

pub(super) fn combine_error(errors: &mut Option<syn::Error>, error: syn::Error) {
    if let Some(errors) = errors {
        errors.combine(error);
    } else {
        *errors = Some(error);
    }
}
