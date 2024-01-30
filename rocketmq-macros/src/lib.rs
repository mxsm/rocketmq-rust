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
use quote::{quote, ToTokens};
use syn::{parse_macro_input, Data, DeriveInput, Fields, Ident, PathArguments, Type};

#[proc_macro_derive(RequestHeaderCodec)]
pub fn request_header_codec(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = &input.ident;
    let fields = match input.data {
        Data::Struct(value) => match value.fields {
            Fields::Named(f) => f.named,
            _ => return quote! {}.into(),
        },
        _ => return quote! {}.into(),
    };

    let (static_fields, to_maps): (Vec<_>, Vec<_>) = fields
        .iter()
        .map(|field| {
            let field_name = field.ident.as_ref().unwrap();
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
                if has_option.is_some() {
                    quote! {
                        if let Some(value) = self.#field_name {
                           map.insert (Self::#static_name.to_string(),value.to_string());
                         }
                    }
                } else {
                    quote! {
                        map.insert (Self::#static_name.to_string(),self.#field_name.to_string());
                    }
                },
            )
        })
        .unzip();

    let from_map = fields
        .iter()
        .map(|field| {
            let field_name = field.ident.as_ref().unwrap();
            let has_option = is_option_type(&field.ty);
            let camel_case_name = snake_to_camel_case(&format!("{}", field_name));
            let static_name = Ident::new(
                &format!("{}", field_name).to_ascii_uppercase(),
                field_name.span(),
            );
            let types = &field.ty;
            if let Some(value) = has_option {
                let type_name = get_type_name(value);
                if type_name == "String" {
                    quote! {
                        quote!{
                            #field_name: map.get(Self::#static_name).cloned(),
                        }
                    }
                }else {
                    quote! {
                        #field_name:map.get(Self::#static_name).and_then(|s| s.parse::<#types>().ok()),
                    }
                }

            } else {
                let type_name = get_type_name(&field.ty);
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
        })
        .collect::<Vec<_>>();

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

            fn from(map: &HashMap<String, String>) -> Option<Self::Target> {
                Some(#struct_name {
                    #(#from_map)*
                })
            }
        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_derive(RemotingSerializable)]
pub fn remoting_serializable(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = &input.ident;

    let expanded = quote! {
        impl crate::protocol::RemotingSerializable for #struct_name {
            type Output = Self;
        }
    };
    TokenStream::from(expanded)
}

fn get_type_name(ty: &Type) -> String {
    ty.to_token_stream().to_string()
}
fn is_option_type(ty: &Type) -> Option<&Type> {
    match ty {
        Type::Path(path) => {
            if let Some(segment) = path.path.segments.last() {
                if segment.ident.to_string() == "Option" {
                    if let PathArguments::AngleBracketed(args) = &segment.arguments {
                        if let Some(arg) = args.args.first() {
                            if let syn::GenericArgument::Type(inner_ty) = arg {
                                return Some(inner_ty);
                            }
                        }
                    }
                }
            }
            None
        }
        _ => None,
    }
}

fn snake_to_camel_case(input: &str) -> String {
    let mut camel_case = String::new();
    let mut capitalize_next = false;

    for c in input.chars() {
        if c == '_' {
            capitalize_next = true;
        } else {
            if capitalize_next {
                camel_case.push(c.to_ascii_uppercase());
                capitalize_next = false;
            } else {
                camel_case.push(c);
            }
        }
    }

    camel_case
}
