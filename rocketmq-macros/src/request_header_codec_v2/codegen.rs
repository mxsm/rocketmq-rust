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

use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};

use super::model::{FieldModel, HeaderModel, TypeCategory};

pub(super) fn generate(model: HeaderModel) -> TokenStream {
    let HeaderModel {
        ident,
        generics,
        fields,
        validation_method,
        protocol_path,
    } = model;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let command_trait = quote!(#protocol_path::protocol::command_custom_header::CommandCustomHeader);
    let from_map_trait = quote!(#protocol_path::protocol::command_custom_header::FromMap);
    let map_type = quote!(#protocol_path::HeaderMap);
    let string_type = quote!(#protocol_path::__request_header_codec::CheetahString);
    let error_type = quote!(#protocol_path::__request_header_codec::RocketMQError);
    let codec_error_type = quote!(#protocol_path::protocol::header_codec::HeaderCodecError);

    let const_decls = fields.iter().filter(|field| !field.flattened).map(gen_const_decl);
    let encode_stmts = fields
        .iter()
        .map(|field| gen_encode(field, &command_trait, &string_type));
    let scalar_capacity = fields.iter().filter(|field| !field.flattened).count();

    // The lookup strategy is fixed at expansion time. Dense headers use one borrowed scan;
    // sparse headers use direct lookups. Alias-bearing fields always use ordered lookups so the
    // canonical key wins regardless of HashMap iteration order.
    let scalar_count = fields.iter().filter(|field| !field.flattened).count();
    let use_scan = scalar_count >= 4;
    let local_decls = fields
        .iter()
        .filter(|field| !field.flattened)
        .map(|field| gen_local_decl(field, use_scan, &map_type, &string_type));
    let scan_arms: Vec<_> = if use_scan {
        fields
            .iter()
            .filter(|field| !field.flattened && field.aliases.is_empty())
            .map(gen_scan_arm)
            .collect()
    } else {
        Vec::new()
    };
    let scan = if scan_arms.is_empty() {
        quote! {}
    } else {
        quote! {
            for (key, value) in map {
                match key.as_str() {
                    #(#scan_arms)*
                    _ => {}
                }
            }
        }
    };
    let construct_fields = fields
        .iter()
        .map(|field| gen_construct(field, &from_map_trait, &error_type));
    let required_string_checks = fields
        .iter()
        .filter(|field| {
            field.required
                && field.inner_type.is_none()
                && matches!(field.category, TypeCategory::CheetahString | TypeCategory::String)
        })
        .map(|field| {
            let field_ident = &field.ident;
            let key = syn::LitStr::new(&field.wire_key, field.span);
            quote! {
                if self.#field_ident.is_empty() {
                    return Err(#error_type::request_header_error(
                        format!("Required header field {} must not be empty", #key),
                    ));
                }
            }
        });
    let custom_validation = validation_method.map(|method| quote!(self.#method()?;));
    let needs_check = fields.iter().any(|field| {
        field.required
            && field.inner_type.is_none()
            && matches!(field.category, TypeCategory::CheetahString | TypeCategory::String)
    }) || custom_validation.is_some();
    let check_fields = if needs_check {
        quote! {
            fn check_fields(&self) -> Result<(), #error_type> {
                #(#required_string_checks)*
                #custom_validation
                Ok(())
            }
        }
    } else {
        quote! {}
    };

    quote! {
        impl #impl_generics #ident #ty_generics #where_clause {
            #(#const_decls)*
        }

        impl #impl_generics #command_trait for #ident #ty_generics #where_clause {
            #check_fields

            fn to_map(&self) -> Option<#map_type> {
                let mut map = #map_type::with_capacity(#scalar_capacity);
                <Self as #command_trait>::encode_into_map(self, &mut map);
                Some(map)
            }

            fn encode_into_map(&self, out: &mut #map_type) {
                #(#encode_stmts)*
            }

            fn try_encode_into_map(&self, out: &mut #map_type) -> Result<(), #codec_error_type> {
                <Self as #command_trait>::check_fields(self).map_err(|_| {
                    #codec_error_type::LegacyValidation {
                        header: ::core::any::type_name::<Self>(),
                    }
                })?;
                <Self as #command_trait>::encode_into_map(self, out);
                Ok(())
            }
        }

        impl #impl_generics #from_map_trait for #ident #ty_generics #where_clause {
            type Error = #error_type;
            type Target = Self;

            fn from(map: &#map_type) -> Result<Self::Target, Self::Error> {
                #(#local_decls)*
                #scan

                let header = Self {
                    #(#construct_fields)*
                };
                <Self as #command_trait>::check_fields(&header)?;
                Ok(header)
            }
        }
    }
}

fn gen_const_decl(field: &FieldModel) -> TokenStream {
    let const_ident = const_ident(field);
    let key = syn::LitStr::new(&field.wire_key, field.span);
    quote!(const #const_ident: &'static str = #key;)
}

fn gen_encode(field: &FieldModel, command_trait: &TokenStream, string_type: &TokenStream) -> TokenStream {
    let field_ident = &field.ident;
    if field.flattened {
        return if field.inner_type.is_some() {
            quote! {
                if let Some(value) = &self.#field_ident {
                    #command_trait::encode_into_map(value, out);
                }
            }
        } else {
            quote!(#command_trait::encode_into_map(&self.#field_ident, out);)
        };
    }

    let const_ident = const_ident(field);
    let insert = |value: TokenStream| {
        quote! {
            out.insert(
                #string_type::from_static_str(Self::#const_ident),
                #value,
            );
        }
    };
    match (field.category, field.inner_type.is_some()) {
        (TypeCategory::CheetahString, true) => {
            let insert = insert(quote!(value.clone()));
            quote!(if let Some(value) = &self.#field_ident { #insert })
        }
        (TypeCategory::CheetahString, false) => insert(quote!(self.#field_ident.clone())),
        (TypeCategory::String, true) => {
            let insert = insert(quote!(#string_type::from_string(value.clone())));
            quote!(if let Some(value) = &self.#field_ident { #insert })
        }
        (TypeCategory::String, false) => insert(quote!(#string_type::from_string(self.#field_ident.clone()))),
        (TypeCategory::Primitive, true) => {
            let insert = insert(quote!(#string_type::from_string(value.to_string())));
            quote!(if let Some(value) = &self.#field_ident { #insert })
        }
        (TypeCategory::Primitive, false) => insert(quote!(#string_type::from_string(self.#field_ident.to_string()))),
        (TypeCategory::Flattened, _) => unreachable!("flattened fields are handled above"),
    }
}

fn gen_local_decl(
    field: &FieldModel,
    use_scan: bool,
    map_type: &TokenStream,
    string_type: &TokenStream,
) -> TokenStream {
    let local = local_ident(field);
    if use_scan && field.aliases.is_empty() {
        return quote!(let mut #local: Option<&#string_type> = None;);
    }

    let const_ident = const_ident(field);
    let aliases: Vec<_> = field
        .aliases
        .iter()
        .map(|alias| syn::LitStr::new(alias, field.span))
        .collect();
    let _ = map_type;
    quote! {
        let #local: Option<&#string_type> = map
            .get(Self::#const_ident)
            #(.or_else(|| map.get(#aliases)))*;
    }
}

fn gen_scan_arm(field: &FieldModel) -> TokenStream {
    let local = local_ident(field);
    let key = syn::LitStr::new(&field.wire_key, field.span);
    quote!(#key => #local = Some(value),)
}

fn gen_construct(field: &FieldModel, from_map_trait: &TokenStream, error_type: &TokenStream) -> TokenStream {
    let field_ident = &field.ident;
    if field.flattened {
        return if let Some(inner) = &field.inner_type {
            quote!(#field_ident: Some(<#inner as #from_map_trait>::from(map)?),)
        } else {
            let ty = &field.ty;
            quote!(#field_ident: <#ty as #from_map_trait>::from(map)?,)
        };
    }

    let local = local_ident(field);
    let missing = syn::LitStr::new(&format!("Missing {} field", field.wire_key), field.span);
    let parse_error = syn::LitStr::new(&format!("Parse {} field error", field.wire_key), field.span);
    let default = field.default_path.as_ref().map_or_else(
        || {
            let ty = &field.ty;
            quote!(<#ty as Default>::default())
        },
        |path| quote!(#path()),
    );

    match (field.category, field.inner_type.as_ref(), field.required) {
        (TypeCategory::CheetahString, Some(_), _) => quote!(#field_ident: #local.cloned(),),
        (TypeCategory::CheetahString, None, true) => quote! {
            #field_ident: #local.cloned().ok_or_else(|| #error_type::request_header_error(#missing.to_string()))?,
        },
        (TypeCategory::CheetahString, None, false) => {
            quote!(#field_ident: #local.cloned().unwrap_or_else(|| #default),)
        }
        (TypeCategory::String, Some(_), _) => quote!(#field_ident: #local.map(ToString::to_string),),
        (TypeCategory::String, None, true) => quote! {
            #field_ident: #local.map(ToString::to_string)
                .ok_or_else(|| #error_type::request_header_error(#missing.to_string()))?,
        },
        (TypeCategory::String, None, false) => quote! {
            #field_ident: #local.map(ToString::to_string).unwrap_or_else(|| #default),
        },
        (TypeCategory::Primitive, Some(inner), _) => quote! {
            #field_ident: match #local {
                Some(value) => value.as_str().parse::<#inner>()
                    .map(Some)
                    .map_err(|_| #error_type::request_header_error(#parse_error.to_string()))?,
                None => None,
            },
        },
        (TypeCategory::Primitive, None, true) => {
            let ty = &field.ty;
            quote! {
                #field_ident: #local
                    .ok_or_else(|| #error_type::request_header_error(#missing.to_string()))?
                    .as_str()
                    .parse::<#ty>()
                    .map_err(|_| #error_type::request_header_error(#parse_error.to_string()))?,
            }
        }
        (TypeCategory::Primitive, None, false) => {
            let ty = &field.ty;
            quote! {
                #field_ident: match #local {
                    Some(value) => value.as_str().parse::<#ty>()
                        .map_err(|_| #error_type::request_header_error(#parse_error.to_string()))?,
                    None => #default,
                },
            }
        }
        (TypeCategory::Flattened, _, _) => unreachable!("flattened fields are handled above"),
    }
}

fn const_ident(field: &FieldModel) -> syn::Ident {
    format_ident!("{}", field.ident.to_string().to_ascii_uppercase(), span = field.span)
}

fn local_ident(field: &FieldModel) -> syn::Ident {
    format_ident!("__request_header_codec_v2_{}", field.ident, span = Span::call_site())
}
