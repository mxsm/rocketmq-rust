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

use proc_macro2::TokenStream;
use quote::quote;

use super::super::model::{FieldModel, HeaderModel};
use crate::get_type_name;

pub(crate) fn generate(model: &HeaderModel) -> TokenStream {
    let struct_name = &model.ident;
    let static_fields = model.fields.iter().map(gen_static_field);
    let to_maps = model.fields.iter().map(gen_to_map);
    let from_map = model.fields.iter().map(gen_from_map);

    quote! {
        impl #struct_name {
            #(#static_fields)*
        }

        impl crate::protocol::command_custom_header::CommandCustomHeader for #struct_name {
            fn to_map(&self) -> Option<std::collections::HashMap<cheetah_string::CheetahString, cheetah_string::CheetahString>> {
                let mut map = std::collections::HashMap::new();
                #(#to_maps)*
                Some(map)
            }
        }

        impl crate::protocol::command_custom_header::FromMap for #struct_name {

            type Error = rocketmq_error::RocketMQError;

            type Target = Self;

            fn from(map: &std::collections::HashMap<cheetah_string::CheetahString, cheetah_string::CheetahString>) -> Result<Self::Target, Self::Error> {
                Ok(#struct_name {
                    #(#from_map)*
                })
            }
        }
    }
}

fn gen_static_field(field: &FieldModel) -> TokenStream {
    let static_name = static_name(field);
    let camel_case_name = &field.key.value;
    quote! {
        const #static_name: &'static str = #camel_case_name;
    }
}

fn gen_to_map(field: &FieldModel) -> TokenStream {
    let field_name = &field.ident;
    let static_name = static_name(field);
    match (legacy_value_kind(field), field.option_inner.is_some(), field.flattened) {
        (_, true, true) => quote! {
            if let Some(ref value) = self.#field_name {
                if let Some(value) = value.to_map() {
                    map.extend(value);
                }
            }
        },
        (LegacyValueKind::CheetahString, true, false) => quote! {
            if let Some(ref value) = self.#field_name {
                map.insert(
                    cheetah_string::CheetahString::from_static_str(Self::#static_name),
                    value.clone()
                );
            }
        },
        (LegacyValueKind::String, true, false) => quote! {
            if let Some(ref value) = self.#field_name {
                map.insert(
                    cheetah_string::CheetahString::from_static_str(Self::#static_name),
                    cheetah_string::CheetahString::from_string(value.clone())
                );
            }
        },
        (LegacyValueKind::Primitive, true, false) => quote! {
            if let Some(ref value) = self.#field_name {
                map.insert(
                    cheetah_string::CheetahString::from_static_str(Self::#static_name),
                    cheetah_string::CheetahString::from_string(value.to_string())
                );
            }
        },
        (_, false, true) => quote! {
            if let Some(value) = self.#field_name.to_map() {
                map.extend(value);
            }
        },
        (LegacyValueKind::CheetahString, false, false) => quote! {
            map.insert(
                cheetah_string::CheetahString::from_static_str(Self::#static_name),
                self.#field_name.clone()
            );
        },
        (LegacyValueKind::String, false, false) => quote! {
            map.insert(
                cheetah_string::CheetahString::from_static_str(Self::#static_name),
                cheetah_string::CheetahString::from_string(self.#field_name.clone())
            );
        },
        (LegacyValueKind::Primitive, false, false) => quote! {
            map.insert(
                cheetah_string::CheetahString::from_static_str(Self::#static_name),
                cheetah_string::CheetahString::from_string(self.#field_name.to_string())
            );
        },
    }
}

fn gen_from_map(field: &FieldModel) -> TokenStream {
    let field_name = &field.ident;
    let static_name = static_name(field);
    let required = field.legacy_required;
    let type_name = legacy_value_kind(field);

    match (field.option_inner.as_ref(), type_name, field.flattened, required) {
        (Some(_), LegacyValueKind::CheetahString, false, true) => quote! {
            #field_name: Some(
                map.get(&cheetah_string::CheetahString::from_static_str(Self::#static_name))
                    .cloned()
                    .ok_or(rocketmq_error::RocketMQError::request_header_error(
                        format!("Missing {} field", Self::#static_name),
                    ))?
            ),
        },
        (Some(_), LegacyValueKind::String, false, true) => quote! {
            Some(
                map.get(&cheetah_string::CheetahString::from_static_str(Self::#static_name))
                    .cloned()
                    .ok_or(rocketmq_error::RocketMQError::request_header_error(
                        format!("Missing {} field", Self::#static_name),
                    ))?
                    .to_string()
            )
        },
        (Some(_), LegacyValueKind::CheetahString | LegacyValueKind::String, false, false) => quote! {
            #field_name: map.get(&cheetah_string::CheetahString::from_static_str(Self::#static_name)).cloned(),
        },
        (Some(type_), _, true, _) => quote! {
            #field_name: Some(<#type_ as crate::protocol::command_custom_header::FromMap>::from(map)?),
        },
        (Some(type_), LegacyValueKind::Primitive, false, true) => quote! {
            #field_name: Some(
                map.get(&cheetah_string::CheetahString::from_static_str(Self::#static_name))
                    .ok_or(rocketmq_error::RocketMQError::request_header_error(
                        format!("Missing {} field", Self::#static_name),
                    ))?
                    .parse::<#type_>()
                    .map_err(|_| rocketmq_error::RocketMQError::request_header_error(
                        format!("Parse {} field error", Self::#static_name)
                    ))?
            ),
        },
        (Some(type_), LegacyValueKind::Primitive, false, false) => quote! {
            #field_name: map.get(&cheetah_string::CheetahString::from_static_str(Self::#static_name))
                .and_then(|s| s.parse::<#type_>().ok()),
        },
        (None, LegacyValueKind::CheetahString, false, true) => quote! {
            #field_name: map.get(&cheetah_string::CheetahString::from_static_str(Self::#static_name))
                .cloned()
                .ok_or(rocketmq_error::RocketMQError::request_header_error(
                    format!("Missing {} field", Self::#static_name),
                ))?,
        },
        (None, LegacyValueKind::String, false, true) => quote! {
            #field_name: map.get(&cheetah_string::CheetahString::from_static_str(Self::#static_name))
                .cloned()
                .ok_or(rocketmq_error::RocketMQError::request_header_error(
                    format!("Missing {} field", Self::#static_name),
                ))?
                .to_string(),
        },
        (None, LegacyValueKind::CheetahString | LegacyValueKind::String, false, false) => quote! {
            #field_name: map.get(&cheetah_string::CheetahString::from_static_str(Self::#static_name))
                .cloned()
                .unwrap_or_default(),
        },
        (None, _, true, _) => {
            let type_ = &field.ty;
            quote! {
                #field_name: <#type_ as crate::protocol::command_custom_header::FromMap>::from(map)?,
            }
        }
        (None, LegacyValueKind::Primitive, false, true) => {
            let type_ = &field.ty;
            quote! {
                #field_name: map.get(&cheetah_string::CheetahString::from_static_str(Self::#static_name))
                    .ok_or(rocketmq_error::RocketMQError::request_header_error(
                        format!("Missing {} field", Self::#static_name),
                    ))?
                    .parse::<#type_>()
                    .map_err(|_| rocketmq_error::RocketMQError::request_header_error(
                        format!("Parse {} field error", Self::#static_name)
                    ))?,
            }
        }
        (None, LegacyValueKind::Primitive, false, false) => {
            let type_ = &field.ty;
            quote! {
                #field_name: map.get(&cheetah_string::CheetahString::from_static_str(Self::#static_name))
                    .and_then(|s| s.parse::<#type_>().ok())
                    .unwrap_or_default(),
            }
        }
    }
}

#[derive(Clone, Copy)]
enum LegacyValueKind {
    CheetahString,
    String,
    Primitive,
}

fn legacy_value_kind(field: &FieldModel) -> LegacyValueKind {
    let type_ = field.option_inner.as_ref().unwrap_or(&field.ty);
    match get_type_name(type_).as_str() {
        "CheetahString" => LegacyValueKind::CheetahString,
        "String" => LegacyValueKind::String,
        _ => LegacyValueKind::Primitive,
    }
}

fn static_name(field: &FieldModel) -> syn::Ident {
    syn::Ident::new(&field.ident.to_string().to_ascii_uppercase(), field.ident.span())
}
