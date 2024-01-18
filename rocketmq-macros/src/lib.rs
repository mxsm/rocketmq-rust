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

mod request_header_custom;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

#[proc_macro]
pub fn request_header(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    //get struct fields
    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            _ => return quote! {}.into(),
        },
        _ => return quote! {}.into(),
    };

    let (static_fields, field_initializers): (Vec<_>, Vec<_>) = fields
        .iter()
        .map(|field| {
            let field_name = field.ident.as_ref().unwrap();
            let static_name = format!("{}", field_name).to_ascii_uppercase();
            (
                quote! {
                    const #static_name: &'static str = stringify!(#field_name);
                },
                quote! {
                    #field_name: #field_name.into(),
                },
            )
        })
        .unzip();

    let to_map_method = {
        let field_mappings = fields.iter().map(|field| {
            let field_name = field.ident.as_ref().unwrap();
            let static_name = format!("{}", field_name).to_ascii_uppercase();
            quote! {
                (
                    #name::#static_name.to_string(),
                    self.#field_name.clone(),
                )
            }
        });
        quote! {
            fn to_map(&self) -> Option<std::collections::HashMap<String, String>> {
                Some(std::collections::HashMap::from([
                    #(#field_mappings,)*
                ]))
            }
        }
    };

    let from_map_method = {
        let field_assignments = fields.iter().map(|field| {
            let field_name = field.ident.as_ref().unwrap();
            let static_name = format!("{}", field_name).to_ascii_uppercase();
            quote! {
                #field_name: map.get(#name::#static_name)?.clone(),
            }
        });
        quote! {
            fn from(map: &std::collections::HashMap<String, String>) -> Option<Self::Target> {
                Some(Self {
                    #(#field_assignments)*
                })
            }
        }
    };
    let expanded: TokenStream2 = quote! {
        impl #name {
            #(#static_fields)*

            pub fn new(#(#fields),*) -> Self {
                Self {
                    #(#field_initializers)*
                }
            }
        }

        impl crate::protocol::command_custom_header::CommandCustomHeader for #name {
            #to_map_method
        }

        impl crate::protocol::command_custom_header::FromMap for #name {
            type Target = Self;
            #from_map_method
        }
    };

    expanded.into()
}
