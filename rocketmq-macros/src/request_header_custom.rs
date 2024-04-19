/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use proc_macro::TokenStream;

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Data, DeriveInput, Fields, Ident, parse_macro_input};

use crate::{get_type_name, is_option_type, snake_to_camel_case};

pub(super) fn request_header_codec_inner(
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = &input.ident;
    //For Java code, all class attributes have names,
    //so all struct attributes of RequestHeader that are compatible with Java have names.
    let fields = match input.data {
        Data::Struct(value) => match value.fields {
            Fields::Named(f) => f.named,
            _ => return quote! {}.into(),
        },
        _ => return quote! {}.into(),
    };

    //build CommandCustomHeader impl
    let (static_fields, (to_maps, from_map)): (Vec<_>, (Vec<_>,Vec<_>)) = fields
        .iter()
        .map(|field| {
            let field_name = field.ident.as_ref().unwrap();
            //Determining whether it is an Option type or a direct data type
            //This will lead to different ways of processing in the future.
            let has_option = is_option_type(&field.ty);
            let camel_case_name = snake_to_camel_case(&format!("{}", field_name));
            let static_name = Ident::new(
                &format!("{}", field_name).to_ascii_uppercase(),
                field_name.span(),
            );

            (
                quote! {
                    const #static_name: &'static str = #camel_case_name;
                },
                (
                    if let Some(value) = has_option {
                        let type_name = get_type_name(value);
                        if type_name == "String" {
                            quote! {
                            if let Some(ref value) = self.#field_name {
                               map.insert (Self::#static_name.to_string(),value.to_string());
                             }
                        }
                        }else {
                            quote! {
                            if let Some(value) = self.#field_name {
                               map.insert (Self::#static_name.to_string(),value.to_string());
                             }
                        }
                        }

                    } else {
                        quote! {
                            map.insert (Self::#static_name.to_string(),self.#field_name.to_string());
                        }
                    },
                    // build FromMap impl
                    if let Some(value) = has_option {
                        let type_name = get_type_name(value);
                        if type_name == "String" {
                            quote! {
                                #field_name: map.get(Self::#static_name).cloned(),
                            }
                        }else {
                            quote! {
                                #field_name:map.get(Self::#static_name).and_then(|s| s.parse::<#value>().ok()),
                            }
                        }

                    } else {
                        let types = &field.ty;
                        let type_name = get_type_name(types);
                        if type_name == "String" {
                            quote!{
                                #field_name: map.get(Self::#static_name).cloned().unwrap_or_default(),
                            }
                        }else {
                            quote! {
                              #field_name:map.get(Self::#static_name).and_then(|s| s.parse::<#types>().ok()).unwrap_or_default(),
                            }
                        }
                        }
                )
            )
        })
        .unzip();
    let expanded: TokenStream2 = quote! {
        impl #struct_name {
            #(#static_fields)*
        }

        impl crate::protocol::command_custom_header::CommandCustomHeader for #struct_name {
            fn to_map(&self) -> Option<std::collections::HashMap<String, String>> {
                let mut map = std::collections::HashMap::new();
                #(#to_maps)*
                Some(map)
            }
        }

        impl crate::protocol::command_custom_header::FromMap for #struct_name {
            type Target = Self;

            fn from(map: &std::collections::HashMap<String, String>) -> Option<Self::Target> {
                Some(#struct_name {
                    #(#from_map)*
                })
            }
        }
    };

    TokenStream::from(expanded)
}
